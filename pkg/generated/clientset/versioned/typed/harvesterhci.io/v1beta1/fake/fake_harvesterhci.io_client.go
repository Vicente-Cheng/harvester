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
	v1beta1 "github.com/harvester/harvester/pkg/generated/clientset/versioned/typed/harvesterhci.io/v1beta1"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeHarvesterhciV1beta1 struct {
	*testing.Fake
}

func (c *FakeHarvesterhciV1beta1) Addons(namespace string) v1beta1.AddonInterface {
	return &FakeAddons{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) KeyPairs(namespace string) v1beta1.KeyPairInterface {
	return &FakeKeyPairs{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) Preferences(namespace string) v1beta1.PreferenceInterface {
	return &FakePreferences{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) ResourceQuotas(namespace string) v1beta1.ResourceQuotaInterface {
	return &FakeResourceQuotas{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) ScheduleVMBackups(namespace string) v1beta1.ScheduleVMBackupInterface {
	return &FakeScheduleVMBackups{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) Settings() v1beta1.SettingInterface {
	return &FakeSettings{c}
}

func (c *FakeHarvesterhciV1beta1) SupportBundles(namespace string) v1beta1.SupportBundleInterface {
	return &FakeSupportBundles{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) Upgrades(namespace string) v1beta1.UpgradeInterface {
	return &FakeUpgrades{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) UpgradeLogs(namespace string) v1beta1.UpgradeLogInterface {
	return &FakeUpgradeLogs{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) Versions(namespace string) v1beta1.VersionInterface {
	return &FakeVersions{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) VirtualMachineBackups(namespace string) v1beta1.VirtualMachineBackupInterface {
	return &FakeVirtualMachineBackups{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) VirtualMachineImages(namespace string) v1beta1.VirtualMachineImageInterface {
	return &FakeVirtualMachineImages{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) VirtualMachineRestores(namespace string) v1beta1.VirtualMachineRestoreInterface {
	return &FakeVirtualMachineRestores{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) VirtualMachineTemplates(namespace string) v1beta1.VirtualMachineTemplateInterface {
	return &FakeVirtualMachineTemplates{c, namespace}
}

func (c *FakeHarvesterhciV1beta1) VirtualMachineTemplateVersions(namespace string) v1beta1.VirtualMachineTemplateVersionInterface {
	return &FakeVirtualMachineTemplateVersions{c, namespace}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeHarvesterhciV1beta1) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}
