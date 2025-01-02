/*
Copyright 2025 Rancher Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by main. DO NOT EDIT.

package fake

import (
	v1 "github.com/harvester/harvester/pkg/generated/clientset/versioned/typed/k8s.cni.cncf.io/v1"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeK8sCniCncfIoV1 struct {
	*testing.Fake
}

func (c *FakeK8sCniCncfIoV1) NetworkAttachmentDefinitions(namespace string) v1.NetworkAttachmentDefinitionInterface {
	return &FakeNetworkAttachmentDefinitions{c, namespace}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeK8sCniCncfIoV1) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}
