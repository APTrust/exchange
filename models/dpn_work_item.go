package models

import (
	"encoding/json"
	"time"
)

// DPNWorkItem contains some basic information about a DPN-related
// task. Valid task values are enumerated in constants/constants.go.
type DPNWorkItem struct {
	Id          int        `json:"id"`
	RemoteNode  string     `json:"remote_node"`
	Task        string     `json:"task"`
	Identifier  string     `json:"identifier"`
	QueuedAt    *time.Time `json:"queued_at"`
	CompletedAt *time.Time `json:"completed_at"`
	Note        *string    `json:"note"`
	State       *string    `json:"state"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// SerializeForPharos serializes a version of DPNWorkItem that Pharos
// will accept as post/put input. The Pharos post/put serialization
// omits some fields that are not allowed by Rails strong params.
func (item *DPNWorkItem) SerializeForPharos() ([]byte, error) {
	data := make(map[string]*DPNWorkItemForPharos)
	data["dpn_work_item"] = NewDPNWorkItemForPharos(item)
	return json.Marshal(data)
}
