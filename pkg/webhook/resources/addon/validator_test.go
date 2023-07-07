package addon

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
)

func Test_validateUpdatedAddon(t *testing.T) {
	var testCases = []struct {
		name          string
		oldAddon      *harvesterv1.Addon
		newAddon      *harvesterv1.Addon
		expectedError bool
	}{
		{
			name: "user can enable addon",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       false,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			expectedError: false,
		},
		{
			name: "user can disable addon",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       false,
					ValuesContent: "sample",
				},
			},
			expectedError: false,
		},
		{
			name: "user cannot change chart field",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1-changed",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			expectedError: true,
		},
		{
			name: "virtual cluster addon with valid dns",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "hostname: rancher.172.19.108.3.sslip.io\nrancherVersion: v2.7.4\nbootstrapPassword: harvesterAdmin\n",
				},
			},
			expectedError: false,
		},
		{
			name: "virtual cluster addon with ingress-expose address",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "hostname: 172.19.108.3\nrancherVersion: v2.7.4\nbootstrapPassword: harvesterAdmin\n",
				},
			},
			expectedError: true,
		},
		{
			name: "virtual cluster addon with invalid fqdn",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "hostname: FakeAddress.com\nrancherVersion: v2.7.4\nbootstrapPassword: harvesterAdmin\n",
				},
			},
			expectedError: true,
		},
		{
			name: "virtual cluster addon empty hostname",
			oldAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name:      vClusterAddonName,
					Namespace: vClusterAddonNamespace,
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "vcluster",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "hostname: \nrancherVersion: v2.7.4\nbootstrapPassword: harvesterAdmin\n",
				},
			},
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		err := validateUpdatedAddon(tc.newAddon, tc.oldAddon)
		if tc.expectedError {
			assert.NotNil(t, err, tc.name)
		} else {
			assert.Nil(t, err, tc.name)
		}
	}
}

func Test_validateNewAddon(t *testing.T) {
	var testCases = []struct {
		name          string
		newAddon      *harvesterv1.Addon
		addonList     []*harvesterv1.Addon
		expectedError bool
	}{
		{
			name: "user can add new addon",
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			addonList:     []*harvesterv1.Addon{},
			expectedError: false,
		},
		{
			name: "user cannot add same addon, no matter differences in version and repo fields",
			newAddon: &harvesterv1.Addon{
				ObjectMeta: metav1.ObjectMeta{
					Name: "addon1",
				},
				Spec: harvesterv1.AddonSpec{
					Repo:          "repo1",
					Chart:         "chart1",
					Version:       "version1",
					Enabled:       true,
					ValuesContent: "sample",
				},
			},
			addonList: []*harvesterv1.Addon{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "addon1",
					},
					Spec: harvesterv1.AddonSpec{
						Repo:          "repo1",
						Chart:         "chart1",
						Version:       "version1",
						Enabled:       true,
						ValuesContent: "sample",
					},
				},
			},
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		err := validateNewAddon(tc.newAddon, tc.addonList)
		if tc.expectedError {
			assert.NotNil(t, err, tc.name)
		} else {
			assert.Nil(t, err, tc.name)
		}
	}
}