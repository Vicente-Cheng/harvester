package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rancher/dynamiclistener"
	"github.com/sirupsen/logrus"

	"github.com/harvester/harvester/pkg/config"
	"github.com/harvester/harvester/pkg/server"
	"github.com/harvester/harvester/tests/framework/cluster"
	"github.com/harvester/harvester/tests/framework/env"
	"github.com/harvester/harvester/tests/framework/helper"
	"github.com/harvester/harvester/tests/integration/runtime"
)

// This command runs only the build-up phase of the integration test suite as a
// self-contained test of the harvester chart installation:
//
//  1. start a kind cluster (or use an existing one),
//  2. construct the harvester runtime (namespaces, CRDs, harvester-crd/harvester charts)
//     and wait until the deployed workloads become ready,
//  3. start the harvester API server and controllers in-process and wait until the
//     v1 API answers.
//
// It is the BeforeSuite of tests/integration/api split out into a standalone entry
// point; it does NOT run any Ginkgo spec.
//
// On success it tears the runtime down and exits 0; on failure it exits non-zero.
// Set KEEP_TESTING_CLUSTER=true to keep the cluster running and block for inspection
// after a successful install (press Ctrl-C to exit).
func main() {
	if err := run(); err != nil {
		logrus.Fatalf("test-chart-installation failed: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logrus.Info("[1/5] starting test cluster (kind)")
	kubeClientConfig, _, err := cluster.Start(os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to start test cluster: %w", err)
	}
	// best-effort teardown on any failure after the cluster is up
	defer teardown()

	kubeConfig, err := kubeClientConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	logrus.Info("[2/5] constructing harvester runtime (namespaces, CRDs, charts)")
	if err := runtime.Construct(ctx, kubeConfig); err != nil {
		return fmt.Errorf("failed to construct harvester runtime: %w", err)
	}

	logrus.Info("[3/5] setting harvester server config")
	options, err := runtime.SetConfig()
	if err != nil {
		return fmt.Errorf("failed to set harvester config: %w", err)
	}

	logrus.Info("[4/5] creating harvester server")
	harvester, err := server.New(ctx, kubeClientConfig, options)
	if err != nil {
		return fmt.Errorf("failed to create harvester server: %w", err)
	}

	logrus.Info("[5/5] starting harvester server (API + controllers)")
	listenOpts := &dynamiclistener.Config{CloseConnOnCertChange: false}
	serveErrChan := make(chan error, 1)
	go func() {
		serveErrChan <- harvester.ListenAndServe(listenOpts, options)
	}()

	apiURL := helper.BuildAPIURL("v1", "", options.HTTPSListenPort)
	if err := waitAPIReady(apiURL, serveErrChan); err != nil {
		return err
	}

	logrus.Info("chart installation verified: harvester charts are ready and the API server is serving")

	// In CI-style runs the test is done here: the deferred teardown cleans up and we
	// exit 0. Only block for manual inspection when explicitly requested.
	if env.IsKeepingTestingCluster() {
		blockForInspection(options, apiURL, serveErrChan)
	}

	return nil
}

// waitAPIReady polls the harvester v1 API until it returns 200, the server exits,
// or the timeout elapses. It mirrors validateAPIIsReady in the api suite.
func waitAPIReady(apiURL string, serveErrChan <-chan error) error {
	logrus.Infof("waiting for harvester API to become ready at %s", apiURL)
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-serveErrChan:
			return fmt.Errorf("harvester server exited before becoming ready: %w", err)
		case <-timeout:
			return fmt.Errorf("timed out waiting for harvester API to be ready")
		case <-ticker.C:
			code, _, err := helper.GetResponse(apiURL)
			if err == nil && code == http.StatusOK {
				return nil
			}
			logrus.Infof("API not ready yet (code=%d, err=%v), retrying...", code, err)
		}
	}
}

// blockForInspection prints connection details and blocks until interrupted or the
// server exits. Only used when KEEP_TESTING_CLUSTER=true.
func blockForInspection(options config.Options, apiURL string, serveErrChan <-chan error) {
	logrus.Info("==================================================================")
	logrus.Info("KEEP_TESTING_CLUSTER=true, runtime is kept for inspection")
	logrus.Infof("  harvester namespace : %s", options.Namespace)
	logrus.Infof("  API server (HTTPS)  : %s", apiURL)
	logrus.Infof("  API server (HTTP)   : http://localhost:%d", options.HTTPListenPort)
	logrus.Info("  kube context        : kind-harvester (override with clusterName env)")
	logrus.Info("  inspect cluster     : kubectl --context kind-harvester get pods -A")
	logrus.Info("  inspect API         : curl -k " + apiURL)
	logrus.Info("Press Ctrl-C to exit (the kind cluster is left running).")
	logrus.Info("==================================================================")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigChan:
		logrus.Infof("received signal %q, exiting", sig)
	case err := <-serveErrChan:
		logrus.Errorf("harvester server exited: %v", err)
	}
}

// teardown stops the test cluster unless it should be kept. cluster.Stop is a no-op
// when USE_EXISTING_CLUSTER or KEEP_TESTING_CLUSTER is set.
func teardown() {
	if env.IsKeepingTestingCluster() {
		logrus.Info("KEEP_TESTING_CLUSTER=true, leaving the kind cluster running")
		return
	}
	logrus.Info("tearing down test cluster")
	if err := cluster.Stop(os.Stdout); err != nil {
		logrus.Errorf("failed to stop test cluster: %v", err)
	}
}
