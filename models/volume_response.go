package models

// VolumeResponse contains response data returned by the VolumeService.
type VolumeResponse struct {
	Succeeded       bool
	ErrorMessage    string
	Data            map[string]uint64
}
