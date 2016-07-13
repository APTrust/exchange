package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
)

func TestClaimedReserveRelease(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := models.NewVolume(filename)
	require.Nil(t, err)
	assert.EqualValues(t, 0, volume.ClaimedSpace())

	err = volume.Reserve("/path/to/file_0", 1000)
	require.Nil(t, err)
	assert.EqualValues(t, 1000, volume.ClaimedSpace())

	volume.Release("/this/file/was/never/reserved")
	assert.EqualValues(t, 1000, volume.ClaimedSpace())

	volume.Release("/path/to/file_0")
	assert.EqualValues(t, 0, volume.ClaimedSpace())
}

// This functional/behavioral test goes through some more realistic
// usage scenarios.
func TestVolume(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := models.NewVolume(filename)
	require.Nil(t, err)

	// Make sure we can reserve space that's actually there.
	initialSpace, err := volume.AvailableSpace()
	require.Nil(t, err)
	numBytes := initialSpace / 3
	err = volume.Reserve("/path/to/file_1", numBytes)
	require.Nil(t, err)
	err = volume.Reserve("/path/to/file_2", numBytes)
	require.Nil(t, err)

	// Make sure we're tracking the available space correctly.
	bytesAvailable, err := volume.AvailableSpace()
	require.Nil(t, err)
	expectedBytesAvailable := (initialSpace - (2 * numBytes))
	assert.Equal(t, expectedBytesAvailable, bytesAvailable)

	// Make sure a request for too much space is rejected
	err = volume.Reserve("/path/to/file_3", numBytes * 2)
	require.NotNil(t, err)

	// Free the two chunks of space we just requested.
	volume.Release("/path/to/file_1")
	volume.Release("/path/to/file_2")

	// Make sure it was freed.
	bytesAvailable, err = volume.AvailableSpace()
	require.Nil(t, err)
	assert.Equal(t, initialSpace, bytesAvailable)

	// Now we should have enough space for this.
	err = volume.Reserve("/path/to/file_4", numBytes * 2)
	require.Nil(t, err)
}
