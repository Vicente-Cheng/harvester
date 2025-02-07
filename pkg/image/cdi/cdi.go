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
	created, err := b.isDataVolumeCreated(vmImg)
	if err != nil {
		return vmImg, fmt.Errorf("failed to check DataVolume: %v", err)
	}
	if created {
		return vmImg, nil
	}

	logrus.Infof("DEBUG: CDI backend Initialize")
	// do nothing when vmimage source is upload, dataVolume will be created by upload handler
	if vmImg.Spec.SourceType == harvesterv1.VirtualMachineImageSourceTypeUpload {
		return vmImg, nil
	}

	logrus.Infof("DEBUG: Initialize doen")
	virtualSize, err := fetchImageVirtualSize(vmImg.Spec.URL)
	if err != nil {
		return vmImg, fmt.Errorf("failed to fetch image virtual size: %v", err)
	}

	size, err := fetchImageSize(vmImg.Spec.URL)
	if err != nil {
		return vmImg, fmt.Errorf("failed to fetch image size: %v", err)
	}

	// means the image is not qcow format
	if virtualSize == 0 {
		virtualSize = size
	}
	logrus.Infof("Image Size: %v", size)
	logrus.Infof("Image Virtual Size: %v", virtualSize)

	if vmImg.Status.Size == 0 && vmImg.Status.VirtualSize == 0 {
		logrus.Infof("Update VM Image size and virtual size before we create the DataVolume")
		vmImgNew := vmImg.DeepCopy()
		vmImgNew.Status.Size = size
		vmImgNew.Status.VirtualSize = virtualSize
		return b.vmio.UpdateVMI(vmImg, vmImgNew)
	}

	dvName := vmImg.ObjectMeta.Name
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
	targetDVName := vmImg.ObjectMeta.Name
	targetDV, err := b.dataVolumeClient.Get(targetDVNs, targetDVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
	}
	if apierrors.IsNotFound(err) {
		logrus.Info("DataVolume not found, waiting for the initialization")
		return err
	}

	// upload source type will update the progress on the upload handler
	if vmImg.Spec.SourceType == harvesterv1.VirtualMachineImageSourceTypeDownload {
		progress := string(targetDV.Status.Progress)
		if progress != "N/A" && progress != "" {
			// progress format looks like "88.82%", we just need the integer part
			parsedInt := strings.Split(progress, ".")[0]
			progressInt, err := strconv.Atoi(parsedInt)
			if err != nil {
				return fmt.Errorf("failed to convert progress to int: %v", err)
			}
			logrus.Infof("Update CDI DataVolume progress: %v", progressInt)
			b.vmio.Importing(vmImg, "Image Importing", progressInt)
		}
		logrus.Infof("CDI DataVolume %s/%s status: %s, progress: %v", targetDVNs, targetDVName, targetDV.Status.Phase, progress)
	}
	if targetDV.Status.Phase != cdiv1.Succeeded {
		return common.ErrRetryLater
	}
	b.vmio.Imported(vmImg, "", 100, vmImg.Status.Size, vmImg.Status.VirtualSize)
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
	targetDVName := vmImg.ObjectMeta.Name
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

func (b *Backend) isDataVolumeCreated(vmImg *harvesterv1.VirtualMachineImage) (bool, error) {
	targetDVNs := vmImg.ObjectMeta.Namespace
	targetDVName := vmImg.ObjectMeta.Name
	_, err := b.dataVolumeClient.Get(targetDVNs, targetDVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("failed to get DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return true, nil
}
