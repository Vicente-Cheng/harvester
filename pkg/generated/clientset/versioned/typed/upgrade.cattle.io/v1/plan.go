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

package v1

import (
	"context"
	"time"

	scheme "github.com/harvester/harvester/pkg/generated/clientset/versioned/scheme"
	v1 "github.com/rancher/system-upgrade-controller/pkg/apis/upgrade.cattle.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// PlansGetter has a method to return a PlanInterface.
// A group's client should implement this interface.
type PlansGetter interface {
	Plans(namespace string) PlanInterface
}

// PlanInterface has methods to work with Plan resources.
type PlanInterface interface {
	Create(ctx context.Context, plan *v1.Plan, opts metav1.CreateOptions) (*v1.Plan, error)
	Update(ctx context.Context, plan *v1.Plan, opts metav1.UpdateOptions) (*v1.Plan, error)
	UpdateStatus(ctx context.Context, plan *v1.Plan, opts metav1.UpdateOptions) (*v1.Plan, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.Plan, error)
	List(ctx context.Context, opts metav1.ListOptions) (*v1.PlanList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.Plan, err error)
	PlanExpansion
}

// plans implements PlanInterface
type plans struct {
	client rest.Interface
	ns     string
}

// newPlans returns a Plans
func newPlans(c *UpgradeV1Client, namespace string) *plans {
	return &plans{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the plan, and returns the corresponding plan object, and an error if there is any.
func (c *plans) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.Plan, err error) {
	result = &v1.Plan{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("plans").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Plans that match those selectors.
func (c *plans) List(ctx context.Context, opts metav1.ListOptions) (result *v1.PlanList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1.PlanList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("plans").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested plans.
func (c *plans) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("plans").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a plan and creates it.  Returns the server's representation of the plan, and an error, if there is any.
func (c *plans) Create(ctx context.Context, plan *v1.Plan, opts metav1.CreateOptions) (result *v1.Plan, err error) {
	result = &v1.Plan{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("plans").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(plan).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a plan and updates it. Returns the server's representation of the plan, and an error, if there is any.
func (c *plans) Update(ctx context.Context, plan *v1.Plan, opts metav1.UpdateOptions) (result *v1.Plan, err error) {
	result = &v1.Plan{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("plans").
		Name(plan.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(plan).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *plans) UpdateStatus(ctx context.Context, plan *v1.Plan, opts metav1.UpdateOptions) (result *v1.Plan, err error) {
	result = &v1.Plan{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("plans").
		Name(plan.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(plan).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the plan and deletes it. Returns an error if one occurs.
func (c *plans) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("plans").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *plans) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("plans").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched plan.
func (c *plans) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.Plan, err error) {
	result = &v1.Plan{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("plans").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
