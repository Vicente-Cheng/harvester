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
	"context"

	v1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeProbes implements ProbeInterface
type FakeProbes struct {
	Fake *FakeMonitoringV1
	ns   string
}

var probesResource = v1.SchemeGroupVersion.WithResource("probes")

var probesKind = v1.SchemeGroupVersion.WithKind("Probe")

// Get takes name of the probe, and returns the corresponding probe object, and an error if there is any.
func (c *FakeProbes) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.Probe, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(probesResource, c.ns, name), &v1.Probe{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.Probe), err
}

// List takes label and field selectors, and returns the list of Probes that match those selectors.
func (c *FakeProbes) List(ctx context.Context, opts metav1.ListOptions) (result *v1.ProbeList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(probesResource, probesKind, c.ns, opts), &v1.ProbeList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1.ProbeList{ListMeta: obj.(*v1.ProbeList).ListMeta}
	for _, item := range obj.(*v1.ProbeList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested probes.
func (c *FakeProbes) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(probesResource, c.ns, opts))

}

// Create takes the representation of a probe and creates it.  Returns the server's representation of the probe, and an error, if there is any.
func (c *FakeProbes) Create(ctx context.Context, probe *v1.Probe, opts metav1.CreateOptions) (result *v1.Probe, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(probesResource, c.ns, probe), &v1.Probe{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.Probe), err
}

// Update takes the representation of a probe and updates it. Returns the server's representation of the probe, and an error, if there is any.
func (c *FakeProbes) Update(ctx context.Context, probe *v1.Probe, opts metav1.UpdateOptions) (result *v1.Probe, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(probesResource, c.ns, probe), &v1.Probe{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.Probe), err
}

// Delete takes name of the probe and deletes it. Returns an error if one occurs.
func (c *FakeProbes) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(probesResource, c.ns, name, opts), &v1.Probe{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeProbes) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(probesResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1.ProbeList{})
	return err
}

// Patch applies the patch and returns the patched probe.
func (c *FakeProbes) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.Probe, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(probesResource, c.ns, name, pt, data, subresources...), &v1.Probe{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.Probe), err
}
