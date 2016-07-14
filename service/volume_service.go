// +build !partners

// Don't include this in the partners build: it's not needed
// in the partner apps, and the syscall.Stat* functions cause
// the build to fail on Windows.
package service

import (
	"github.com/APTrust/exchange/models"
	"github.com/op/go-logging"
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
