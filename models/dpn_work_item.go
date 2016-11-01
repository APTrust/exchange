package models

import (
	"time"
)

// DPNWorkItem contains some basic information about a DPN-related
// task. Valid task values are enumerated in constants/constants.go.
type DPNWorkItem struct {
	Id                     int                  `json:"id"`
	Node                   string               `json:"node"`
	Task                   string               `json:"task"`
	Identifier             string               `json:"identifier"`
	QueuedAt               *time.Time           `json:"queued_at"`
	CompletedAt            *time.Time           `json:"completed_at"`
	Note                   *string              `json:"note"`
	State                  *string              `json:"state"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
}
