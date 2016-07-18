// +build !partners

package service_test

import (
	"fmt"
	"github.com/APTrust/exchange/service"
	"github.com/APTrust/exchange/util/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
//	"net/http/httptest"
	"net/url"
	"testing"
)

var port = 8818
var serviceUrl = fmt.Sprintf("http://127.0.0.1:%d", port)
var volumeService *service.VolumeService

func runService(t *testing.T) {
	if volumeService == nil {
		log := logger.DiscardLogger("test_volume_service")
		volumeService = service.NewVolumeService(port, log)
		require.NotNil(t, volumeService)
		go volumeService.Serve()
	}
}

func TestNewVolumeService(t *testing.T) {
	runService(t)
}

func TestReserve(t *testing.T) {
	runService(t)

	reserveUrl := fmt.Sprintf("%s/reserve/", serviceUrl)

	// Start with a good request
	params := url.Values{
		"path": {"/tmp/some_file"},
		"bytes": {"8000"},
	}
	resp, err := http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":true,"ErrorMessage":""}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Bad request: no path
	params = url.Values{
		"bytes": {"8000"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'path' is required."}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Bad request: no value for bytes
	params = url.Values{
		"path": {"/tmp/some_file"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'bytes' must be an integer greater than zero."}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRelease(t *testing.T) {
	runService(t)

	reserveUrl := fmt.Sprintf("%s/release/", serviceUrl)

	// Good request
	params := url.Values{
		"path": {"/tmp/some_file"},
	}
	resp, err := http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":true,"ErrorMessage":""}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Bad request - no path
	params = url.Values{
		"useless_param": {"/tmp/some_file"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'path' is required."}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestReport(t *testing.T) {

}
