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

package v1beta1

import (
	"context"
	"sync"
	"time"

	v1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/condition"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/kv"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// KeyPairController interface for managing KeyPair resources.
type KeyPairController interface {
	generic.ControllerInterface[*v1beta1.KeyPair, *v1beta1.KeyPairList]
}

// KeyPairClient interface for managing KeyPair resources in Kubernetes.
type KeyPairClient interface {
	generic.ClientInterface[*v1beta1.KeyPair, *v1beta1.KeyPairList]
}

// KeyPairCache interface for retrieving KeyPair resources in memory.
type KeyPairCache interface {
	generic.CacheInterface[*v1beta1.KeyPair]
}

// KeyPairStatusHandler is executed for every added or modified KeyPair. Should return the new status to be updated
type KeyPairStatusHandler func(obj *v1beta1.KeyPair, status v1beta1.KeyPairStatus) (v1beta1.KeyPairStatus, error)

// KeyPairGeneratingHandler is the top-level handler that is executed for every KeyPair event. It extends KeyPairStatusHandler by a returning a slice of child objects to be passed to apply.Apply
type KeyPairGeneratingHandler func(obj *v1beta1.KeyPair, status v1beta1.KeyPairStatus) ([]runtime.Object, v1beta1.KeyPairStatus, error)

// RegisterKeyPairStatusHandler configures a KeyPairController to execute a KeyPairStatusHandler for every events observed.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterKeyPairStatusHandler(ctx context.Context, controller KeyPairController, condition condition.Cond, name string, handler KeyPairStatusHandler) {
	statusHandler := &keyPairStatusHandler{
		client:    controller,
		condition: condition,
		handler:   handler,
	}
	controller.AddGenericHandler(ctx, name, generic.FromObjectHandlerToHandler(statusHandler.sync))
}

// RegisterKeyPairGeneratingHandler configures a KeyPairController to execute a KeyPairGeneratingHandler for every events observed, passing the returned objects to the provided apply.Apply.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterKeyPairGeneratingHandler(ctx context.Context, controller KeyPairController, apply apply.Apply,
	condition condition.Cond, name string, handler KeyPairGeneratingHandler, opts *generic.GeneratingHandlerOptions) {
	statusHandler := &keyPairGeneratingHandler{
		KeyPairGeneratingHandler: handler,
		apply:                    apply,
		name:                     name,
		gvk:                      controller.GroupVersionKind(),
	}
	if opts != nil {
		statusHandler.opts = *opts
	}
	controller.OnChange(ctx, name, statusHandler.Remove)
	RegisterKeyPairStatusHandler(ctx, controller, condition, name, statusHandler.Handle)
}

type keyPairStatusHandler struct {
	client    KeyPairClient
	condition condition.Cond
	handler   KeyPairStatusHandler
}

// sync is executed on every resource addition or modification. Executes the configured handlers and sends the updated status to the Kubernetes API
func (a *keyPairStatusHandler) sync(key string, obj *v1beta1.KeyPair) (*v1beta1.KeyPair, error) {
	if obj == nil {
		return obj, nil
	}

	origStatus := obj.Status.DeepCopy()
	obj = obj.DeepCopy()
	newStatus, err := a.handler(obj, obj.Status)
	if err != nil {
		// Revert to old status on error
		newStatus = *origStatus.DeepCopy()
	}

	if a.condition != "" {
		if errors.IsConflict(err) {
			a.condition.SetError(&newStatus, "", nil)
		} else {
			a.condition.SetError(&newStatus, "", err)
		}
	}
	if !equality.Semantic.DeepEqual(origStatus, &newStatus) {
		if a.condition != "" {
			// Since status has changed, update the lastUpdatedTime
			a.condition.LastUpdated(&newStatus, time.Now().UTC().Format(time.RFC3339))
		}

		var newErr error
		obj.Status = newStatus
		newObj, newErr := a.client.UpdateStatus(obj)
		if err == nil {
			err = newErr
		}
		if newErr == nil {
			obj = newObj
		}
	}
	return obj, err
}

type keyPairGeneratingHandler struct {
	KeyPairGeneratingHandler
	apply apply.Apply
	opts  generic.GeneratingHandlerOptions
	gvk   schema.GroupVersionKind
	name  string
	seen  sync.Map
}

// Remove handles the observed deletion of a resource, cascade deleting every associated resource previously applied
func (a *keyPairGeneratingHandler) Remove(key string, obj *v1beta1.KeyPair) (*v1beta1.KeyPair, error) {
	if obj != nil {
		return obj, nil
	}

	obj = &v1beta1.KeyPair{}
	obj.Namespace, obj.Name = kv.RSplit(key, "/")
	obj.SetGroupVersionKind(a.gvk)

	if a.opts.UniqueApplyForResourceVersion {
		a.seen.Delete(key)
	}

	return nil, generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects()
}

// Handle executes the configured KeyPairGeneratingHandler and pass the resulting objects to apply.Apply, finally returning the new status of the resource
func (a *keyPairGeneratingHandler) Handle(obj *v1beta1.KeyPair, status v1beta1.KeyPairStatus) (v1beta1.KeyPairStatus, error) {
	if !obj.DeletionTimestamp.IsZero() {
		return status, nil
	}

	objs, newStatus, err := a.KeyPairGeneratingHandler(obj, status)
	if err != nil {
		return newStatus, err
	}
	if !a.isNewResourceVersion(obj) {
		return newStatus, nil
	}

	err = generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects(objs...)
	if err != nil {
		return newStatus, err
	}
	a.storeResourceVersion(obj)
	return newStatus, nil
}

// isNewResourceVersion detects if a specific resource version was already successfully processed.
// Only used if UniqueApplyForResourceVersion is set in generic.GeneratingHandlerOptions
func (a *keyPairGeneratingHandler) isNewResourceVersion(obj *v1beta1.KeyPair) bool {
	if !a.opts.UniqueApplyForResourceVersion {
		return true
	}

	// Apply once per resource version
	key := obj.Namespace + "/" + obj.Name
	previous, ok := a.seen.Load(key)
	return !ok || previous != obj.ResourceVersion
}

// storeResourceVersion keeps track of the latest resource version of an object for which Apply was executed
// Only used if UniqueApplyForResourceVersion is set in generic.GeneratingHandlerOptions
func (a *keyPairGeneratingHandler) storeResourceVersion(obj *v1beta1.KeyPair) {
	if !a.opts.UniqueApplyForResourceVersion {
		return
	}

	key := obj.Namespace + "/" + obj.Name
	a.seen.Store(key, obj.ResourceVersion)
}
