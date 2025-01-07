package cdi

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	ctlcdiv1 "github.com/harvester/harvester/pkg/generated/controllers/cdi.kubevirt.io/v1beta1"
	"github.com/harvester/harvester/pkg/image/backend"
	"github.com/harvester/harvester/pkg/image/common"
)

type Backend struct {
	ctx              context.Context
	dataVolumeClient ctlcdiv1.DataVolumeClient
	vmio             common.VMIOperator

	dataVolumeCreated bool
}

func GetBackend(ctx context.Context, dataVolumeClient ctlcdiv1.DataVolumeClient, vmio common.VMIOperator) backend.Backend {
	return &Backend{
		ctx:               ctx,
		dataVolumeClient:  dataVolumeClient,
		vmio:              vmio,
		dataVolumeCreated: false,
	}
}

func (b *Backend) Initialize(vmImg *harvesterv1.VirtualMachineImage) (*harvesterv1.VirtualMachineImage, error) {
	// if dataVolume is already created, return
	if b.dataVolumeCreated {
		return vmImg, nil
	}

	dvName := vmImg.Spec.DisplayName
	dvNamespace := vmImg.ObjectMeta.Namespace

	// generate DV source
	dvSource, err := generateDVSource(vmImg)
	if err != nil {
		return vmImg, fmt.Errorf("failed to generate DV source: %v", err)
	}

	// generate DV target storage
	dvTargetStorage, err := generateDVTargetStorage(vmImg)
	if err != nil {
		return vmImg, fmt.Errorf("failed to generate DV target storage: %v", err)
	}
	dataVolumeTemplate := &cdiv1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dvName,
			Namespace: dvNamespace,
		},
		Spec: cdiv1.DataVolumeSpec{
			Source:  dvSource,
			Storage: dvTargetStorage,
		},
	}
	logrus.Infof("Prepare to create DV, storage: %+v", dataVolumeTemplate.Spec.Storage.Resources.Requests)
	if _, err := b.dataVolumeClient.Create(dataVolumeTemplate); err != nil {
		return vmImg, fmt.Errorf("failed to create DataVolume %s/%s: %v", dvNamespace, dvName, err)
	}
	logrus.Infof("DataVolume %s/%s created", dvNamespace, dvName)

	vmImg.Status.ImageVolume = dvName

	return vmImg, nil
}

func (b *Backend) Check(vmImg *harvesterv1.VirtualMachineImage) error {
	logrus.Infof("Running CDI backend check")
	targetDVNs := vmImg.ObjectMeta.Namespace
	targetDVName := vmImg.Spec.DisplayName
	targetDV, err := b.dataVolumeClient.Get(targetDVNs, targetDVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
	}
	if apierrors.IsNotFound(err) {
		b.dataVolumeCreated = false
		logrus.Info("DataVolume not found, waiting for the initinali")
		return err
	}
	b.dataVolumeCreated = true
	progress := string(targetDV.Status.Progress)
	Status := targetDV.Status.Phase
	if progress != "N/A" {
		// progress format looks like "88.82%", we just need the integer part
		parsedInt := strings.Split(progress, ".")[0]
		progressInt, err := strconv.Atoi(parsedInt)
		if err != nil {
			return fmt.Errorf("failed to convert progress to int: %v", err)
		}
		logrus.Infof("Update CDI DataVolume progress: %v", progressInt)
		b.vmio.Importing(vmImg, "Image Importing", progressInt)
	}
	logrus.Infof("CDI DataVolume %s/%s status: %s, progress: %v", targetDVNs, targetDVName, Status, progress)
	if Status != cdiv1.Succeeded {
		return common.ErrRetryLater
	}
	b.vmio.Imported(vmImg, "Image Imported", 100, -1, -1)
	return nil
}

func (b *Backend) UpdateVirtualSize(vmi *harvesterv1.VirtualMachineImage) (*harvesterv1.VirtualMachineImage, error) {
	//ToDo: Implement this function
	logrus.Warnf("CDI backend UpdateVirtualSize is not implemented")
	return vmi, nil
}

func (b *Backend) Delete(vmImg *harvesterv1.VirtualMachineImage) error {
	logrus.Infof("Execute CDI backend Delete")
	targetDVNs := vmImg.ObjectMeta.Namespace
	targetDVName := vmImg.Spec.DisplayName
	_, err := b.dataVolumeClient.Get(targetDVNs, targetDVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
	}
	if apierrors.IsNotFound(err) {
		logrus.Infof("Target DataVolume (%s/%s) is already gone do no-op found", targetDVNs, targetDVName)
		return nil
	}

	// delete the DataVolume
	if err := b.dataVolumeClient.Delete(targetDVNs, targetDVName, &metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("failed to delete DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
	}
	return nil
}

func (b *Backend) AddSidecarHandler() {
}
