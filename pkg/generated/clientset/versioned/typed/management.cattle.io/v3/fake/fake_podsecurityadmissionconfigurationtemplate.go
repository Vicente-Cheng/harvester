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

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakePodSecurityAdmissionConfigurationTemplates implements PodSecurityAdmissionConfigurationTemplateInterface
type FakePodSecurityAdmissionConfigurationTemplates struct {
	Fake *FakeManagementV3
}

var podsecurityadmissionconfigurationtemplatesResource = v3.SchemeGroupVersion.WithResource("podsecurityadmissionconfigurationtemplates")

var podsecurityadmissionconfigurationtemplatesKind = v3.SchemeGroupVersion.WithKind("PodSecurityAdmissionConfigurationTemplate")

// Get takes name of the podSecurityAdmissionConfigurationTemplate, and returns the corresponding podSecurityAdmissionConfigurationTemplate object, and an error if there is any.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Get(ctx context.Context, name string, options v1.GetOptions) (result *v3.PodSecurityAdmissionConfigurationTemplate, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootGetAction(podsecurityadmissionconfigurationtemplatesResource, name), &v3.PodSecurityAdmissionConfigurationTemplate{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.PodSecurityAdmissionConfigurationTemplate), err
}

// List takes label and field selectors, and returns the list of PodSecurityAdmissionConfigurationTemplates that match those selectors.
func (c *FakePodSecurityAdmissionConfigurationTemplates) List(ctx context.Context, opts v1.ListOptions) (result *v3.PodSecurityAdmissionConfigurationTemplateList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootListAction(podsecurityadmissionconfigurationtemplatesResource, podsecurityadmissionconfigurationtemplatesKind, opts), &v3.PodSecurityAdmissionConfigurationTemplateList{})
	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v3.PodSecurityAdmissionConfigurationTemplateList{ListMeta: obj.(*v3.PodSecurityAdmissionConfigurationTemplateList).ListMeta}
	for _, item := range obj.(*v3.PodSecurityAdmissionConfigurationTemplateList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested podSecurityAdmissionConfigurationTemplates.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchAction(podsecurityadmissionconfigurationtemplatesResource, opts))
}

// Create takes the representation of a podSecurityAdmissionConfigurationTemplate and creates it.  Returns the server's representation of the podSecurityAdmissionConfigurationTemplate, and an error, if there is any.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Create(ctx context.Context, podSecurityAdmissionConfigurationTemplate *v3.PodSecurityAdmissionConfigurationTemplate, opts v1.CreateOptions) (result *v3.PodSecurityAdmissionConfigurationTemplate, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateAction(podsecurityadmissionconfigurationtemplatesResource, podSecurityAdmissionConfigurationTemplate), &v3.PodSecurityAdmissionConfigurationTemplate{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.PodSecurityAdmissionConfigurationTemplate), err
}

// Update takes the representation of a podSecurityAdmissionConfigurationTemplate and updates it. Returns the server's representation of the podSecurityAdmissionConfigurationTemplate, and an error, if there is any.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Update(ctx context.Context, podSecurityAdmissionConfigurationTemplate *v3.PodSecurityAdmissionConfigurationTemplate, opts v1.UpdateOptions) (result *v3.PodSecurityAdmissionConfigurationTemplate, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateAction(podsecurityadmissionconfigurationtemplatesResource, podSecurityAdmissionConfigurationTemplate), &v3.PodSecurityAdmissionConfigurationTemplate{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.PodSecurityAdmissionConfigurationTemplate), err
}

// Delete takes name of the podSecurityAdmissionConfigurationTemplate and deletes it. Returns an error if one occurs.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteActionWithOptions(podsecurityadmissionconfigurationtemplatesResource, name, opts), &v3.PodSecurityAdmissionConfigurationTemplate{})
	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakePodSecurityAdmissionConfigurationTemplates) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionAction(podsecurityadmissionconfigurationtemplatesResource, listOpts)

	_, err := c.Fake.Invokes(action, &v3.PodSecurityAdmissionConfigurationTemplateList{})
	return err
}

// Patch applies the patch and returns the patched podSecurityAdmissionConfigurationTemplate.
func (c *FakePodSecurityAdmissionConfigurationTemplates) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v3.PodSecurityAdmissionConfigurationTemplate, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceAction(podsecurityadmissionconfigurationtemplatesResource, name, pt, data, subresources...), &v3.PodSecurityAdmissionConfigurationTemplate{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.PodSecurityAdmissionConfigurationTemplate), err
}
