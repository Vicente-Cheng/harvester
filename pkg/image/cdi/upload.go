package cdi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	uploadcdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/upload/v1beta1"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	ctlcdiv1 "github.com/harvester/harvester/pkg/generated/controllers/cdi.kubevirt.io/v1beta1"
	ctlharvesterv1 "github.com/harvester/harvester/pkg/generated/controllers/harvesterhci.io/v1beta1"
	ctlcdiuploadv1 "github.com/harvester/harvester/pkg/generated/controllers/upload.cdi.kubevirt.io/v1beta1"
	"github.com/harvester/harvester/pkg/image/backend"
	"github.com/harvester/harvester/pkg/image/common"
)

const (
	CDIUploadURLRaw = "cdi-uploadproxy.harvester-system"
	UploadProxyURI  = "/v1alpha1/upload"
)

type Uploader struct {
	vmImgClient      ctlharvesterv1.VirtualMachineImageClient
	dataVolumeClient ctlcdiv1.DataVolumeClient
	cdiUploadClient  ctlcdiuploadv1.UploadTokenRequestClient
	httpClient       http.Client
	vmio             common.VMIOperator
}

func GetUploader(vmImgClient ctlharvesterv1.VirtualMachineImageClient,
	dataVolumeClient ctlcdiv1.DataVolumeClient,
	cdiUploadClient ctlcdiuploadv1.UploadTokenRequestClient,
	httpClient http.Client,
	vmio common.VMIOperator) backend.Uploader {

	// set insecure as default
	httpClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &Uploader{
		vmImgClient:      vmImgClient,
		dataVolumeClient: dataVolumeClient,
		cdiUploadClient:  cdiUploadClient,
		httpClient:       httpClient,
		vmio:             vmio,
	}
}

func (cu *Uploader) Do(vmImg *harvesterv1.VirtualMachineImage, req *http.Request) error {
	var err error
	defer func() {
		if err != nil {
			if updateErr := cu.vmio.FailUpload(vmImg, err.Error()); updateErr != nil {
				logrus.Error(err)
			}
		}
	}()

	updaterLocker := &sync.Mutex{}
	updaterCond := sync.NewCond(updaterLocker)

	urlParams := req.URL.Query()
	fileSizeStr := urlParams.Get("size")
	fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse file size: %v", err)
	}
	logrus.Infof("[DEBUG] fileSize: %v", fileSize)
	logrus.Infof("[DEBUG] req: %+v", req)
	virtualSize := int64(fileSize)

	// no matter multipart or not, the first 4k would be enough
	// to find the magic number and virtual size
	tmpBuff := make([]byte, 1024)
	len, err := req.Body.Read(tmpBuff)
	if err != nil && err != io.EOF {
		logrus.Infof("[DEBUG]: err: %v", err)
	}

	rawContent := tmpBuff[:len]
	logrus.Infof("Read %d bytes from the request body", len)

	headerEnd := []byte("\r\n\r\n")
	headerEndIndex := bytes.Index(rawContent, headerEnd)
	logrus.Infof("[DEBUG] headerEndIndex: %v", headerEndIndex)
	// try to find the magic number of first 4096 bytes
	// the multipart body will contain the boundary string and the headers.
	// We should still find the magic number in the first 4096 bytes
	qcowMagic := []byte("QFI\xfb")
	index := bytes.Index(rawContent, qcowMagic)
	logrus.Infof("[DEBUG]: first 1024 bytes: %v", string(rawContent))
	if index == -1 {
		logrus.Infof("Magic number is not correct: %v, this image is not qcow format", rawContent)
	} else {
		// The virtual size is at 24-31 bytes (from the qcow image header)
		logrus.Infof("[DEBUG] index: %v", index)
		virtualSizeRaw := rawContent[index+24 : index+32]
		virtualSize = int64(binary.BigEndian.Uint64(virtualSizeRaw))
		logrus.Infof("[DEBUG] virtualSize: %v", virtualSize)
	}
	dataContent := rawContent[headerEndIndex+4:]
	vmImgNew := vmImg.DeepCopy()
	vmImgNew.Status.Size = fileSize
	vmImgNew.Status.VirtualSize = virtualSize
	if _, err := cu.vmio.UpdateVMI(vmImg, vmImgNew); err != nil {
		return fmt.Errorf("failed to update VM Image: %v", err)
	}

	// check VMImage status again (for size/virtual size)
	if err := wait.PollUntilContextTimeout(context.Background(), tickPolling, tickTimeout, true, func(context.Context) (bool, error) {
		return cu.waitVMImageStatus(vmImg, fileSize, virtualSize)
	}); err != nil {
		return fmt.Errorf("failed to wait for VMImage status: %v", err)
	}

	logrus.Infof("Pass waiting for VMImage status, start to create DataVolume")
	// get the latest VMImage
	vmImg, err = cu.vmImgClient.Get(vmImg.Namespace, vmImg.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get VMImage: %v", err)
	}

	// create DataVolume
	dvName := vmImg.ObjectMeta.Name
	dvNamespace := vmImg.ObjectMeta.Namespace

	// generate DV source
	dvSource, err := generateDVSource(vmImg)
	if err != nil {
		return fmt.Errorf("failed to generate DV source: %v", err)
	}

	// generate DV target storage
	dvTargetStorage, err := generateDVTargetStorage(vmImg)
	if err != nil {
		return fmt.Errorf("failed to generate DV target storage: %v", err)
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
	if _, err := cu.dataVolumeClient.Create(dataVolumeTemplate); err != nil {
		return fmt.Errorf("failed to create DataVolume %s/%s: %v", dvNamespace, dvName, err)
	}
	logrus.Infof("DataVolume %s/%s created", dvNamespace, dvName)

	// wait data volume UploadReady
	if err := wait.PollUntilContextTimeout(context.Background(), tickPolling, tickTimeout, true, func(context.Context) (bool, error) {
		return cu.waitDataVolumeStatus(dvNamespace, dvName, cdiv1.UploadReady)
	}); err != nil {
		return fmt.Errorf("failed to wait for VMImage status: %v", err)
	}

	uploadTokenRequest := &uploadcdiv1.UploadTokenRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "token-for-upload",
			Namespace: dvNamespace,
		},
		Spec: uploadcdiv1.UploadTokenRequestSpec{
			PvcName: dvName,
		},
	}
	retUploadTokenRequest, err := cu.cdiUploadClient.Create(uploadTokenRequest)
	if err != nil {
		return fmt.Errorf("failed to create UploadTokenRequest %s/%s: %v", dvNamespace, dvName, err)
	}
	token := retUploadTokenRequest.Status.Token
	logrus.Infof("UploadTokenRequest %s/%s created, token: %v", dvNamespace, dvName, token)

	logrus.Infof("Pass waiting for DataVolume status, start to create create new upload request")

	//cdiUploadEndpoint := fmt.Sprintf("https://%s", CDIUploadURLRaw)
	cdiUploadURL := fmt.Sprintf("https://%s%s", CDIUploadURLRaw, UploadProxyURI)
	//cdiUploaURL, err := constructUploadProxyPathAsync(cu.httpClient, cdiUploadEndpoint, token)
	logrus.Infof("Upload URL: %v", cdiUploadURL)

	//uploadReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, cdiUploaURL, req.Body)

	newBody := io.MultiReader(bytes.NewReader(dataContent), req.Body)

	progress := &ProgressUpdater{
		targetBytes:       fileSize,
		lastTime:          time.Now(),
		vmImgUpdateLocker: updaterLocker,
		vmImgCond:         updaterCond,
	}
	tee := io.TeeReader(newBody, progress)
	uploadReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, cdiUploadURL, io.NopCloser(tee))
	//uploadReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, cdiUploadURL, io.NopCloser(newBody))

	//uploadReq, _ := http.NewRequest("POST", "https://cdi-uploadproxy.harvester-system/v1alpha1/upload", io.NopCloser(newBody))
	// Reassign r.Body to our new composite reader
	//uploadReq.Body = io.NopCloser(newBody)

	uploadReq.Header = req.Header
	uploadReq.Header.Add("Authorization", "Bearer "+token)
	//uploadReq.Header.Add("Content-Type", "application/octet-stream")
	//uploadReq.ContentLength = fileSize
	//uploadReq.URL.RawQuery = req.URL.RawQuery

	// create VMI progress updater
	go cu.updateVMImageProgress(vmImg, updaterCond, progress, fileSize)

	var urlErr *url.Error
	uploadResp, err := cu.httpClient.Do(uploadReq)
	if errors.As(err, &urlErr) {
		// Trim the "POST http://xxx" implementation detail for the error
		// set the err var and it will be recorded in image condition in the defer function
		err = errors.Unwrap(urlErr)
		return err
	} else if err != nil {
		return fmt.Errorf("failed to send the upload request: %w", err)
	}
	defer uploadResp.Body.Close()

	// create VMI progress updater

	body, err := ioutil.ReadAll(uploadResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if uploadResp.StatusCode >= http.StatusBadRequest {
		// err will be recorded in image condition in the defer function
		err = fmt.Errorf("upload failed: %s", string(body))
		return err
	}

	// final wake up, we need to wait the DataVolume status to be succeeded
	// ensure the VMImage status is updated, wake up the updater
	if err := wait.PollUntilContextTimeout(context.Background(), tickPolling, tickTimeout, true, func(context.Context) (bool, error) {
		return cu.waitDataVolumeStatus(dvNamespace, dvName, cdiv1.Succeeded)
	}); err != nil {
		return fmt.Errorf("failed to wait for VMImage status: %v", err)
	}
	updaterLocker.Lock()
	logrus.Infof("Final wake up, the DataVolume status is succeeded")
	updaterCond.Signal()
	updaterLocker.Unlock()

	return nil
}

func (cu *Uploader) waitVMImageStatus(vmImg *harvesterv1.VirtualMachineImage, size, virtualSize int64) (bool, error) {
	logrus.Infof("waitVMImageStatus: %v, %v, %v", vmImg.Name, size, virtualSize)
	vmImg, err := cu.vmImgClient.Get(vmImg.Namespace, vmImg.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	logrus.Infof("Current VMImage status: size(%d), virtual size(%d), target size(%d), virtual size(%d)", vmImg.Status.Size, vmImg.Status.VirtualSize, size, virtualSize)
	if vmImg.Status.Size == size && vmImg.Status.VirtualSize == virtualSize {
		return true, nil
	}
	logrus.Warnf("Current VMImage status: size(%d), virtual size(%d), target size(%d), virtual size(%d)", vmImg.Status.Size, vmImg.Status.VirtualSize, size, virtualSize)
	return false, nil
}

func (cu *Uploader) waitDataVolumeStatus(namespace, name string, targetState cdiv1.DataVolumePhase) (bool, error) {
	dv, err := cu.dataVolumeClient.Get(namespace, name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if dv.Status.Phase == targetState {
		return true, nil
	}
	logrus.Warnf("Current DataVolume %s/%s status: %v, target status: %v", namespace, name, dv.Status.Phase, targetState)
	return false, nil
}

func (cu *Uploader) updateVMImageProgress(vmImg *harvesterv1.VirtualMachineImage, cond *sync.Cond, updater *ProgressUpdater, targetSize int64) {
	for {
		cond.L.Lock()
		logrus.Infof("DEBUG: Waiting for signal to update VMImage progress")
		cond.Wait()

		var err error
		// ensure the vmImage is the latest
		vmImg, err = cu.vmImgClient.Get(vmImg.Namespace, vmImg.Name, metav1.GetOptions{})
		if err != nil {
			logrus.Errorf("[updateVMImageProgress] failed to get VMImage %s/%s: %v, ignore the latest", vmImg.Namespace, vmImg.Name, err)
			cond.L.Unlock()
			continue
		}

		// check if the upload is finished
		targetDVNs := vmImg.ObjectMeta.Namespace
		targetDVName := vmImg.ObjectMeta.Name
		targetDV, err := cu.dataVolumeClient.Get(targetDVNs, targetDVName, metav1.GetOptions{})
		if err != nil {
			logrus.Errorf("[updateVMImageProgress] failed to get DataVolume %s/%s: %v", targetDVNs, targetDVName, err)
			cond.L.Unlock()
			continue
		}
		if targetDV.Status.Phase == cdiv1.Succeeded {
			logrus.Infof("DataVolume %s/%s upload finished", targetDVNs, targetDVName)
			cond.L.Unlock()
			cu.vmio.Imported(vmImg, "", 100, vmImg.Status.Size, vmImg.Status.VirtualSize)
			return
		}

		currentBytes := updater.GetCurrentBytesNoLock()
		progress := (float64(currentBytes) / float64(targetSize)) * 100
		if progress >= 100 {
			// keep almost done progress, we need to wait the DataVolume status to be succeeded
			progress = 99
		}
		logrus.Infof("DataVolume %s/%s upload progress: %d/%d (%v)", targetDVNs, targetDVName, currentBytes, targetSize, progress)
		_, err = cu.vmio.Importing(vmImg, "Image Importing", int(progress))
		if err != nil {
			logrus.Errorf("failed to update VMImage status: %v", err)
		}
		cond.L.Unlock()
	}
}
