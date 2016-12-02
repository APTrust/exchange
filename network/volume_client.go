package network

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type VolumeClient struct {
	serviceUrl string
}

func NewVolumeClient(port int) *VolumeClient {
	return &VolumeClient{
		serviceUrl: fmt.Sprintf("http://127.0.0.1:%d", port),
	}
}

func (client *VolumeClient) BaseURL() string {
	return client.serviceUrl
}

func (client *VolumeClient) Ping(msTimeout int) error {
	pingUrl := fmt.Sprintf("%s/ping/", client.serviceUrl)
	timeout := time.Duration(time.Duration(msTimeout) * time.Millisecond)
	httpClient := http.Client{
		Timeout: timeout,
	}
	_, err := httpClient.Get(pingUrl)
	return err
}

func (client *VolumeClient) Reserve(path string, bytes uint64) (bool, error) {
	if path == "" {
		return false, fmt.Errorf("Path cannot be empty.")
	}
	if bytes < uint64(1) {
		return false, fmt.Errorf("You must request at least one byte of storage.")
	}
	reserveUrl := fmt.Sprintf("%s/reserve/", client.serviceUrl)
	params := url.Values{
		"path":  {path},
		"bytes": {strconv.FormatUint(bytes, 10)},
	}
	return client.doRequest(reserveUrl, params)
}

func (client *VolumeClient) Release(path string) error {
	releaseUrl := fmt.Sprintf("%s/release/", client.serviceUrl)
	if path == "" {
		return fmt.Errorf("Path cannot be empty.")
	}
	params := url.Values{
		"path": {path},
	}
	_, err := client.doRequest(releaseUrl, params)
	return err
}

func (client *VolumeClient) doRequest(url string, params url.Values) (bool, error) {
	resp, err := http.PostForm(url, params)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	volumeResponse := &models.VolumeResponse{}
	err = json.Unmarshal(data, volumeResponse)
	if err != nil {
		return false, err
	}
	if volumeResponse.ErrorMessage != "" {
		return false, fmt.Errorf(volumeResponse.ErrorMessage)
	}
	return volumeResponse.Succeeded, nil
}

func (client *VolumeClient) Report(path string) (map[string]uint64, error) {
	if path == "" {
		return nil, fmt.Errorf("Path cannot be empty.")
	}
	reportUrl := fmt.Sprintf("%s/report/?path=%s", client.serviceUrl, path)
	resp, err := http.Get(reportUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	volumeResponse := &models.VolumeResponse{}
	err = json.Unmarshal(data, volumeResponse)
	if err != nil {
		return nil, err
	}
	if volumeResponse.ErrorMessage != "" {
		return nil, fmt.Errorf(volumeResponse.ErrorMessage)
	}
	return volumeResponse.Data, nil
}
