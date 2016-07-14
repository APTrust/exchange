// +build !partners

// Don't include this in the partners build: it's not needed
// in the partner apps, and the syscall.Stat* functions inside
// the models.Volume code cause the build to fail on Windows.
package service

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/op/go-logging"
	"net/http"
)

type VolumeService struct {
	port        int
	volumes     map[string]*models.Volume
	messageLog  *logging.Logger
}

// NewVolumeService creates a new VolumeService object to track the
// amount of available space and claimed space on locally mounted
// volumes.
func NewVolumeService(port int, messageLog *logging.Logger) (*VolumeService) {
	return &VolumeService{
		port: port,
		volumes: make(map[string]*models.Volume),
		messageLog: messageLog,
	}
}

// Returns a Volume object with info about the volume at the specified
// mount point. The mount point should be the path to a disk or partition.
// For example, "/", "/mnt/data", etc.
func (service *VolumeService) getVolume(mountPoint string) (*models.Volume) {
	if _, keyExists := service.volumes[mountPoint]; !keyExists {
		service.volumes[mountPoint] = models.NewVolume(mountPoint)
	}
	return service.volumes[mountPoint]
}

func reserveHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "")
}

func releaseHandler(w http.ResponseWriter, r *http.Request) {

}

func reportHandler(w http.ResponseWriter, r *http.Request) {

}
