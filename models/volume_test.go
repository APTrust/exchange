package models_test

import (
	"github.com/APTrust/exchange/models"
	"runtime"
	"testing"
)

func TestInitialFreeSpace(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := models.NewVolume(filename)
	if err != nil {
		t.Errorf("Cannot get file system's available space: %v\n", err)
	}
	initialSpace := volume.InitialFreeSpace()
	if initialSpace <= 0 {
		t.Error("InitialSpace() returned zero")
	}
}

func TestClaimedReserveRelease(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := models.NewVolume(filename)
	if err != nil {
		t.Errorf("Cannot get file system's available space: %v\n", err)
	}
	if volume.ClaimedSpace() != 0 {
		t.Error("Claimed space should be zero")
	}
	err = volume.Reserve(1000)
	if err != nil {
		t.Errorf("Reserve returned error: %v\n", err)
	}
	if volume.ClaimedSpace() != 1000 {
		t.Errorf("Claimed space should be 1000, returned %d", volume.ClaimedSpace())
	}
	volume.Release(1000)
	if volume.ClaimedSpace() != 0 {
		t.Error("Claimed space should be zero")
	}
}

// This functional/behavioral test goes through some more realistic
// usage scenarios.
func TestVolume(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	volume, err := models.NewVolume(filename)
	if err != nil {
		t.Errorf("Cannot get file system's available space: %v\n", err)
	}

	// Make sure we can reserve space that's actually there.
	initialSpace := volume.InitialFreeSpace()
	numBytes := initialSpace / 3
	err = volume.Reserve(numBytes)
	if err != nil {
		t.Errorf("Reserve rejected first reservation request: %v", err)
	}
	err = volume.Reserve(numBytes)
	if err != nil {
		t.Errorf("Reserve rejected second reservation request: %v", err)
	}

	// Make sure we're tracking the available space correctly.
	bytesAvailable := volume.AvailableSpace()
	if err != nil {
		t.Errorf("AvailableSpace() returned error: %v", err)
	}
	expectedBytesAvailable := (initialSpace - (2 * numBytes))
	if bytesAvailable != expectedBytesAvailable {
		t.Errorf("Available space was calculated incorrectly after Reserve: was %d, expected %d",
			bytesAvailable, expectedBytesAvailable)
	}

	// Make sure a request for too much space is rejected
	err = volume.Reserve(numBytes * 2)
	if err == nil {
		t.Error("Reserve should have rejected third reservation request")
	}

	// Free the two chunks of space we just requested.
	volume.Release(numBytes)
	volume.Release(numBytes)

	// Make sure it was freed.
	if volume.AvailableSpace() != volume.InitialFreeSpace() {
		t.Errorf("Available space was calculated incorrectly after Release: "+
			"was %d, expected something close to %d",
			volume.AvailableSpace(), volume.InitialFreeSpace())
	}

	// Now we should have enough space for this.
	err = volume.Reserve(numBytes * 2)
	if err != nil {
		t.Errorf("Reserve rejected final reservation request: %v", err)
	}

}
