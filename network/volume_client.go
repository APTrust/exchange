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

// VolumeClient connects to the VolumeService, which keeps track of how much
// disk space is used/available in our staging area. Workers use this
// service to determine whether there is enough disk space to start work on
// a job. We don't even want to start downloading a 250GB bag if we're
// going to run out of disk space before the download complete. Doing so
// will likely cause other worker tasks to fail due to lack of disk space.
type VolumeClient struct {
	serviceUrl string
}

// NewVolumeClient returns a new VolumeClient. Param port is
// the port number on which the service is running. That info should be
// available in config.VolumeServicePort.
func NewVolumeClient(port int) *VolumeClient {
	return &VolumeClient{
		serviceUrl: fmt.Sprintf("http://127.0.0.1:%d", port),
	}
}

// BaseURL returns the base URL of the VolumeService, which should
// always be running on localhost. (The service has to be able to stat
// local disks, so it should be running on localhost.)
func (client *VolumeClient) BaseURL() string {
	return client.serviceUrl
}

// Ping sends a message to the VolumeService to see if it's running.
// If the service isn't running, you'll get an error. Otherwise,
// in the immortal words of Judge Spaulding Smails,
// "You'll get nothing and like it."
func (client *VolumeClient) Ping(msTimeout int) error {
	pingUrl := fmt.Sprintf("%s/ping/", client.serviceUrl)
	timeout := time.Duration(time.Duration(msTimeout) * time.Millisecond)
	httpClient := http.Client{
		Timeout: timeout,
	}
	_, err := httpClient.Get(pingUrl)
	return err
}

// Reserve tells the VolumeService that you want to reserve space on the
// local staging volume. Param path is the file path you're reserving space
// for, and bytes is the number of bytes you want to reserve.
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

// Release tells the VolumeService that you're done with whatever disk space
// you reserved for the file at path.
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

// Report returns information about all current disk space reservations
// from the VolumeService. In the map this function returns, the keys are
// file paths, and the values are the number of bytes reserved for those
// file paths.
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
