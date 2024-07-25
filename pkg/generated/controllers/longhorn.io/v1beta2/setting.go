/*
Copyright 2024 Rancher Labs, Inc.

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

package v1beta2

import (
	"context"
	"sync"
	"time"

	v1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/rancher/lasso/pkg/client"
	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/wrangler/pkg/apply"
	"github.com/rancher/wrangler/pkg/condition"
	"github.com/rancher/wrangler/pkg/generic"
	"github.com/rancher/wrangler/pkg/kv"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type SettingHandler func(string, *v1beta2.Setting) (*v1beta2.Setting, error)

type SettingController interface {
	generic.ControllerMeta
	SettingClient

	OnChange(ctx context.Context, name string, sync SettingHandler)
	OnRemove(ctx context.Context, name string, sync SettingHandler)
	Enqueue(namespace, name string)
	EnqueueAfter(namespace, name string, duration time.Duration)

	Cache() SettingCache
}

type SettingClient interface {
	Create(*v1beta2.Setting) (*v1beta2.Setting, error)
	Update(*v1beta2.Setting) (*v1beta2.Setting, error)
	UpdateStatus(*v1beta2.Setting) (*v1beta2.Setting, error)
	Delete(namespace, name string, options *metav1.DeleteOptions) error
	Get(namespace, name string, options metav1.GetOptions) (*v1beta2.Setting, error)
	List(namespace string, opts metav1.ListOptions) (*v1beta2.SettingList, error)
	Watch(namespace string, opts metav1.ListOptions) (watch.Interface, error)
	Patch(namespace, name string, pt types.PatchType, data []byte, subresources ...string) (result *v1beta2.Setting, err error)
}

type SettingCache interface {
	Get(namespace, name string) (*v1beta2.Setting, error)
	List(namespace string, selector labels.Selector) ([]*v1beta2.Setting, error)

	AddIndexer(indexName string, indexer SettingIndexer)
	GetByIndex(indexName, key string) ([]*v1beta2.Setting, error)
}

type SettingIndexer func(obj *v1beta2.Setting) ([]string, error)

type settingController struct {
	controller    controller.SharedController
	client        *client.Client
	gvk           schema.GroupVersionKind
	groupResource schema.GroupResource
}

func NewSettingController(gvk schema.GroupVersionKind, resource string, namespaced bool, controller controller.SharedControllerFactory) SettingController {
	c := controller.ForResourceKind(gvk.GroupVersion().WithResource(resource), gvk.Kind, namespaced)
	return &settingController{
		controller: c,
		client:     c.Client(),
		gvk:        gvk,
		groupResource: schema.GroupResource{
			Group:    gvk.Group,
			Resource: resource,
		},
	}
}

func FromSettingHandlerToHandler(sync SettingHandler) generic.Handler {
	return func(key string, obj runtime.Object) (ret runtime.Object, err error) {
		var v *v1beta2.Setting
		if obj == nil {
			v, err = sync(key, nil)
		} else {
			v, err = sync(key, obj.(*v1beta2.Setting))
		}
		if v == nil {
			return nil, err
		}
		return v, err
	}
}

func (c *settingController) Updater() generic.Updater {
	return func(obj runtime.Object) (runtime.Object, error) {
		newObj, err := c.Update(obj.(*v1beta2.Setting))
		if newObj == nil {
			return nil, err
		}
		return newObj, err
	}
}

func UpdateSettingDeepCopyOnChange(client SettingClient, obj *v1beta2.Setting, handler func(obj *v1beta2.Setting) (*v1beta2.Setting, error)) (*v1beta2.Setting, error) {
	if obj == nil {
		return obj, nil
	}

	copyObj := obj.DeepCopy()
	newObj, err := handler(copyObj)
	if newObj != nil {
		copyObj = newObj
	}
	if obj.ResourceVersion == copyObj.ResourceVersion && !equality.Semantic.DeepEqual(obj, copyObj) {
		return client.Update(copyObj)
	}

	return copyObj, err
}

func (c *settingController) AddGenericHandler(ctx context.Context, name string, handler generic.Handler) {
	c.controller.RegisterHandler(ctx, name, controller.SharedControllerHandlerFunc(handler))
}

func (c *settingController) AddGenericRemoveHandler(ctx context.Context, name string, handler generic.Handler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), handler))
}

func (c *settingController) OnChange(ctx context.Context, name string, sync SettingHandler) {
	c.AddGenericHandler(ctx, name, FromSettingHandlerToHandler(sync))
}

func (c *settingController) OnRemove(ctx context.Context, name string, sync SettingHandler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), FromSettingHandlerToHandler(sync)))
}

func (c *settingController) Enqueue(namespace, name string) {
	c.controller.Enqueue(namespace, name)
}

func (c *settingController) EnqueueAfter(namespace, name string, duration time.Duration) {
	c.controller.EnqueueAfter(namespace, name, duration)
}

func (c *settingController) Informer() cache.SharedIndexInformer {
	return c.controller.Informer()
}

func (c *settingController) GroupVersionKind() schema.GroupVersionKind {
	return c.gvk
}

func (c *settingController) Cache() SettingCache {
	return &settingCache{
		indexer:  c.Informer().GetIndexer(),
		resource: c.groupResource,
	}
}

func (c *settingController) Create(obj *v1beta2.Setting) (*v1beta2.Setting, error) {
	result := &v1beta2.Setting{}
	return result, c.client.Create(context.TODO(), obj.Namespace, obj, result, metav1.CreateOptions{})
}

func (c *settingController) Update(obj *v1beta2.Setting) (*v1beta2.Setting, error) {
	result := &v1beta2.Setting{}
	return result, c.client.Update(context.TODO(), obj.Namespace, obj, result, metav1.UpdateOptions{})
}

func (c *settingController) UpdateStatus(obj *v1beta2.Setting) (*v1beta2.Setting, error) {
	result := &v1beta2.Setting{}
	return result, c.client.UpdateStatus(context.TODO(), obj.Namespace, obj, result, metav1.UpdateOptions{})
}

func (c *settingController) Delete(namespace, name string, options *metav1.DeleteOptions) error {
	if options == nil {
		options = &metav1.DeleteOptions{}
	}
	return c.client.Delete(context.TODO(), namespace, name, *options)
}

func (c *settingController) Get(namespace, name string, options metav1.GetOptions) (*v1beta2.Setting, error) {
	result := &v1beta2.Setting{}
	return result, c.client.Get(context.TODO(), namespace, name, result, options)
}

func (c *settingController) List(namespace string, opts metav1.ListOptions) (*v1beta2.SettingList, error) {
	result := &v1beta2.SettingList{}
	return result, c.client.List(context.TODO(), namespace, result, opts)
}

func (c *settingController) Watch(namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return c.client.Watch(context.TODO(), namespace, opts)
}

func (c *settingController) Patch(namespace, name string, pt types.PatchType, data []byte, subresources ...string) (*v1beta2.Setting, error) {
	result := &v1beta2.Setting{}
	return result, c.client.Patch(context.TODO(), namespace, name, pt, data, result, metav1.PatchOptions{}, subresources...)
}

type settingCache struct {
	indexer  cache.Indexer
	resource schema.GroupResource
}

func (c *settingCache) Get(namespace, name string) (*v1beta2.Setting, error) {
	obj, exists, err := c.indexer.GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(c.resource, name)
	}
	return obj.(*v1beta2.Setting), nil
}

func (c *settingCache) List(namespace string, selector labels.Selector) (ret []*v1beta2.Setting, err error) {

	err = cache.ListAllByNamespace(c.indexer, namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta2.Setting))
	})

	return ret, err
}

func (c *settingCache) AddIndexer(indexName string, indexer SettingIndexer) {
	utilruntime.Must(c.indexer.AddIndexers(map[string]cache.IndexFunc{
		indexName: func(obj interface{}) (strings []string, e error) {
			return indexer(obj.(*v1beta2.Setting))
		},
	}))
}

func (c *settingCache) GetByIndex(indexName, key string) (result []*v1beta2.Setting, err error) {
	objs, err := c.indexer.ByIndex(indexName, key)
	if err != nil {
		return nil, err
	}
	result = make([]*v1beta2.Setting, 0, len(objs))
	for _, obj := range objs {
		result = append(result, obj.(*v1beta2.Setting))
	}
	return result, nil
}

// SettingStatusHandler is executed for every added or modified Setting. Should return the new status to be updated
type SettingStatusHandler func(obj *v1beta2.Setting, status v1beta2.SettingStatus) (v1beta2.SettingStatus, error)

// SettingGeneratingHandler is the top-level handler that is executed for every Setting event. It extends SettingStatusHandler by a returning a slice of child objects to be passed to apply.Apply
type SettingGeneratingHandler func(obj *v1beta2.Setting, status v1beta2.SettingStatus) ([]runtime.Object, v1beta2.SettingStatus, error)

// RegisterSettingStatusHandler configures a SettingController to execute a SettingStatusHandler for every events observed.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterSettingStatusHandler(ctx context.Context, controller SettingController, condition condition.Cond, name string, handler SettingStatusHandler) {
	statusHandler := &settingStatusHandler{
		client:    controller,
		condition: condition,
		handler:   handler,
	}
	controller.AddGenericHandler(ctx, name, FromSettingHandlerToHandler(statusHandler.sync))
}

// RegisterSettingGeneratingHandler configures a SettingController to execute a SettingGeneratingHandler for every events observed, passing the returned objects to the provided apply.Apply.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterSettingGeneratingHandler(ctx context.Context, controller SettingController, apply apply.Apply,
	condition condition.Cond, name string, handler SettingGeneratingHandler, opts *generic.GeneratingHandlerOptions) {
	statusHandler := &settingGeneratingHandler{
		SettingGeneratingHandler: handler,
		apply:                    apply,
		name:                     name,
		gvk:                      controller.GroupVersionKind(),
	}
	if opts != nil {
		statusHandler.opts = *opts
	}
	controller.OnChange(ctx, name, statusHandler.Remove)
	RegisterSettingStatusHandler(ctx, controller, condition, name, statusHandler.Handle)
}

type settingStatusHandler struct {
	client    SettingClient
	condition condition.Cond
	handler   SettingStatusHandler
}

// sync is executed on every resource addition or modification. Executes the configured handlers and sends the updated status to the Kubernetes API
func (a *settingStatusHandler) sync(key string, obj *v1beta2.Setting) (*v1beta2.Setting, error) {
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

type settingGeneratingHandler struct {
	SettingGeneratingHandler
	apply apply.Apply
	opts  generic.GeneratingHandlerOptions
	gvk   schema.GroupVersionKind
	name  string
	seen  sync.Map
}

// Remove handles the observed deletion of a resource, cascade deleting every associated resource previously applied
func (a *settingGeneratingHandler) Remove(key string, obj *v1beta2.Setting) (*v1beta2.Setting, error) {
	if obj != nil {
		return obj, nil
	}

	obj = &v1beta2.Setting{}
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

// Handle executes the configured SettingGeneratingHandler and pass the resulting objects to apply.Apply, finally returning the new status of the resource
func (a *settingGeneratingHandler) Handle(obj *v1beta2.Setting, status v1beta2.SettingStatus) (v1beta2.SettingStatus, error) {
	if !obj.DeletionTimestamp.IsZero() {
		return status, nil
	}

	objs, newStatus, err := a.SettingGeneratingHandler(obj, status)
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
func (a *settingGeneratingHandler) isNewResourceVersion(obj *v1beta2.Setting) bool {
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
func (a *settingGeneratingHandler) storeResourceVersion(obj *v1beta2.Setting) {
	if !a.opts.UniqueApplyForResourceVersion {
		return
	}

	key := obj.Namespace + "/" + obj.Name
	a.seen.Store(key, obj.ResourceVersion)
}
