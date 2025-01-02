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

// FakeGroupMembers implements GroupMemberInterface
type FakeGroupMembers struct {
	Fake *FakeManagementV3
}

var groupmembersResource = v3.SchemeGroupVersion.WithResource("groupmembers")

var groupmembersKind = v3.SchemeGroupVersion.WithKind("GroupMember")

// Get takes name of the groupMember, and returns the corresponding groupMember object, and an error if there is any.
func (c *FakeGroupMembers) Get(ctx context.Context, name string, options v1.GetOptions) (result *v3.GroupMember, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootGetAction(groupmembersResource, name), &v3.GroupMember{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.GroupMember), err
}

// List takes label and field selectors, and returns the list of GroupMembers that match those selectors.
func (c *FakeGroupMembers) List(ctx context.Context, opts v1.ListOptions) (result *v3.GroupMemberList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootListAction(groupmembersResource, groupmembersKind, opts), &v3.GroupMemberList{})
	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v3.GroupMemberList{ListMeta: obj.(*v3.GroupMemberList).ListMeta}
	for _, item := range obj.(*v3.GroupMemberList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested groupMembers.
func (c *FakeGroupMembers) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchAction(groupmembersResource, opts))
}

// Create takes the representation of a groupMember and creates it.  Returns the server's representation of the groupMember, and an error, if there is any.
func (c *FakeGroupMembers) Create(ctx context.Context, groupMember *v3.GroupMember, opts v1.CreateOptions) (result *v3.GroupMember, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateAction(groupmembersResource, groupMember), &v3.GroupMember{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.GroupMember), err
}

// Update takes the representation of a groupMember and updates it. Returns the server's representation of the groupMember, and an error, if there is any.
func (c *FakeGroupMembers) Update(ctx context.Context, groupMember *v3.GroupMember, opts v1.UpdateOptions) (result *v3.GroupMember, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateAction(groupmembersResource, groupMember), &v3.GroupMember{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.GroupMember), err
}

// Delete takes name of the groupMember and deletes it. Returns an error if one occurs.
func (c *FakeGroupMembers) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteActionWithOptions(groupmembersResource, name, opts), &v3.GroupMember{})
	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeGroupMembers) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionAction(groupmembersResource, listOpts)

	_, err := c.Fake.Invokes(action, &v3.GroupMemberList{})
	return err
}

// Patch applies the patch and returns the patched groupMember.
func (c *FakeGroupMembers) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v3.GroupMember, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceAction(groupmembersResource, name, pt, data, subresources...), &v3.GroupMember{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v3.GroupMember), err
}
