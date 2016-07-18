package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/service"
	"github.com/APTrust/exchange/util/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var volTestPort = 8558
var volumeService *service.VolumeService

func runService(t *testing.T) {
	if volumeService == nil {
		log := logger.DiscardLogger("test_volume_service")
		volumeService = service.NewVolumeService(volTestPort, log)
		require.NotNil(t, volumeService)
		go volumeService.Serve()
	}
}

func TestNewVolumeClient(t *testing.T) {
	client := network.NewVolumeClient(volTestPort)
	require.NotNil(t, client)
	expectedUrl := fmt.Sprintf("http://127.0.0.1:%d", volTestPort)
	assert.Equal(t, expectedUrl, client.BaseURL())
}

func TestVolumeReserve(t *testing.T) {
	runService(t)
	client := network.NewVolumeClient(volTestPort)
	require.NotNil(t, client)

	ok, err := client.Reserve("/tmp/some_file", uint64(800))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = client.Reserve("", uint64(800))
	assert.NotNil(t, err) // path required
	assert.False(t, ok)

	ok, err = client.Reserve("", uint64(0))
	assert.NotNil(t, err) // > 0 bytes required
	assert.False(t, ok)
}

func TestVolumeRelease(t *testing.T) {
	runService(t)
	client := network.NewVolumeClient(volTestPort)
	require.NotNil(t, client)

	err := client.Release("/tmp/some_file")
	assert.Nil(t, err)

	err = client.Release("")
	assert.NotNil(t, err) // path required
}

func TestVolumeReport(t *testing.T) {
	runService(t)
	client := network.NewVolumeClient(volTestPort)
	require.NotNil(t, client)

	data, err := client.Report("/tmp/some_file")
	assert.Nil(t, err)
	assert.NotNil(t, data)

	data, err = client.Report("")
	assert.NotNil(t, err) // path required
	assert.Nil(t, data)
}

func TestPing(t *testing.T) {
	runService(t)
	client := network.NewVolumeClient(volTestPort)
	require.NotNil(t, client)

	err := client.Ping()
	assert.Nil(t, err)
}
