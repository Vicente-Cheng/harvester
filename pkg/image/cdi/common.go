package cdi

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
)

const (
	VolumeModeBlock = "Block"
	AccessModeRWO   = "ReadWriteOnce"
)

func generateDVSource(vmi *harvesterv1.VirtualMachineImage) (*cdiv1.DataVolumeSource, error) {
	dvSource := &cdiv1.DataVolumeSource{}
	sourceType := vmi.Spec.SourceType
	switch sourceType {
	case harvesterv1.VirtualMachineImageSourceTypeDownload:
		dvSourceHTTP := generateDVSourceHTTP(vmi)
		dvSource.HTTP = dvSourceHTTP
		return dvSource, nil
	case harvesterv1.VirtualMachineImageSourceTypeUpload:
		return dvSource, fmt.Errorf("upload source type is not implemented")
	default:
		return dvSource, fmt.Errorf("unsupported source type: %s", sourceType)
	}
}

func generateDVSourceHTTP(vmi *harvesterv1.VirtualMachineImage) *cdiv1.DataVolumeSourceHTTP {
	return &cdiv1.DataVolumeSourceHTTP{
		URL: vmi.Spec.URL,
	}
}

func generateDVTargetStorage(vmi *harvesterv1.VirtualMachineImage) (*cdiv1.StorageSpec, error) {
	defaultVolMode := func() *corev1.PersistentVolumeMode {
		mode := corev1.PersistentVolumeBlock
		return &mode
	}
	targetDVStorage := &cdiv1.StorageSpec{}
	targetDVStorage.StorageClassName = &vmi.Spec.TargetStorageClassName
	targetDVStorage.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	targetDVStorage.VolumeMode = defaultVolMode()
	targetDVStorage.Resources.Requests = make(corev1.ResourceList)
	targetDVStorage.Resources.Requests[corev1.ResourceStorage] = *resource.NewQuantity(vmi.Spec.CDITargetVolumeSize, resource.DecimalSI)
	return targetDVStorage, nil
}
