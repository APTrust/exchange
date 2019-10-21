// +build !partners

// Package service is not included  in the partners build: it's not
// needed in the partner apps, and the syscall.Stat* functions inside
// the models.Volume code cause the build to fail on Windows.
package service

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/platform"
	"github.com/op/go-logging"
	"net/http"
	"strconv"
)

// VolumeService keeps track of the space available to workers
// processing APTrust bags.
type VolumeService struct {
	port    int
	volumes map[string]*models.Volume
	logger  *logging.Logger
}

// NewVolumeService creates a new VolumeService object to track the
// amount of available space and claimed space on locally mounted
// volumes.
func NewVolumeService(port int, logger *logging.Logger) *VolumeService {
	return &VolumeService{
		port:    port,
		volumes: make(map[string]*models.Volume),
		logger:  logger,
	}
}

// Serve starts an HTTP server, so the VolumeService can respond to
// requests from the VolumeClient(s). See the VolumeClient for available
// calls.
func (service *VolumeService) Serve() {
	http.HandleFunc("/reserve/", service.makeReserveHandler())
	http.HandleFunc("/release/", service.makeReleaseHandler())
	http.HandleFunc("/report/", service.makeReportHandler())
	http.HandleFunc("/ping/", service.makePingHandler())
	listenAddr := fmt.Sprintf("127.0.0.1:%d", service.port)
	http.ListenAndServe(listenAddr, nil)
}

// Returns a Volume object with info about the volume at the specified
// mount point. The mount point should be the path to a disk or partition.
// For example, "/", "/mnt/data", etc.
func (service *VolumeService) getVolume(path string) *models.Volume {
	mountpoint, err := platform.GetMountPointFromPath(path)
	if err != nil {
		mountpoint = "/"
		service.logger.Error("Cannot determine mountpoint of file '%s': %v",
			path, err)
	}
	if _, keyExists := service.volumes[mountpoint]; !keyExists {
		service.volumes[mountpoint] = models.NewVolume(mountpoint)
	}
	return service.volumes[mountpoint]
}

func (service *VolumeService) makeReserveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &models.VolumeResponse{}
		status := http.StatusOK
		path := r.FormValue("path")
		bytes, err := strconv.ParseUint(r.FormValue("bytes"), 10, 64)
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else if err != nil || bytes < 1 {
			response.Succeeded = false
			response.ErrorMessage = "Param 'bytes' must be an integer greater than zero."
			status = http.StatusBadRequest
		} else {
			volume := service.getVolume(path)
			err = volume.Reserve(path, bytes)
			if err != nil {
				response.Succeeded = false
				response.ErrorMessage = fmt.Sprintf(
					"Could not reserve %d bytes for file '%s': %v",
					bytes, path, err)
				service.logger.Error("[%s] %s", r.RemoteAddr, response.ErrorMessage)
				status = http.StatusInternalServerError
			} else {
				response.Succeeded = true
				service.logger.Info("[%s] Reserved %d bytes for %s", r.RemoteAddr, bytes, path)
			}
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (service *VolumeService) makeReleaseHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &models.VolumeResponse{}
		path := r.FormValue("path")
		status := http.StatusOK
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else {
			volume := service.getVolume(path)
			volume.Release(path)
			response.Succeeded = true
			service.logger.Info("[%s] Released %s", r.RemoteAddr, path)
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (service *VolumeService) makeReportHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &models.VolumeResponse{}
		path := r.FormValue("path")
		status := http.StatusOK
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else {
			volume := service.getVolume(path)
			response.Succeeded = true
			response.Data = volume.Reservations()
			service.logger.Info("[%s] Reservations (%d)", r.RemoteAddr, path, len(response.Data))
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (service *VolumeService) makePingHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &models.VolumeResponse{}
		response.Succeeded = true
		status := http.StatusOK
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}
