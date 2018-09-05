package models

import (
	"encoding/json"
	"os"
	"time"
)

// DPNWorkItem contains some basic information about a DPN-related
// task. Valid task values are enumerated in constants/constants.go.
type DPNWorkItem struct {
	Id             int        `json:"id"`
	RemoteNode     string     `json:"remote_node"`
	Task           string     `json:"task"`
	Identifier     string     `json:"identifier"`
	QueuedAt       *time.Time `json:"queued_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	ProcessingNode *string    `json:"processing_node"`
	Pid            int        `json:"pid"`
	Stage          string     `json:"stage"`
	Status         string     `json:"status"`
	Retry          bool       `json:"retry"`
	Note           *string    `json:"note"`
	State          *string    `json:"state"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// SerializeForPharos serializes a version of DPNWorkItem that Pharos
// will accept as post/put input. The Pharos post/put serialization
// omits some fields that are not allowed by Rails strong params.
func (item *DPNWorkItem) SerializeForPharos() ([]byte, error) {
	data := make(map[string]*DPNWorkItemForPharos)
	data["dpn_work_item"] = NewDPNWorkItemForPharos(item)
	return json.Marshal(data)
}

// IsBeingProcessed returns true if this item is currently being
// processed by any node.
func (item *DPNWorkItem) IsBeingProcessed() bool {
	return item.ProcessingNode != nil && item.Pid != 0
}

// IsBeingProcessedByMe returns true if this item is currently
// being processed by the specified hostname under the specified pid.
func (item *DPNWorkItem) IsBeingProcessedByMe(hostname string, pid int) bool {
	return item.ProcessingNode != nil && *item.ProcessingNode == hostname && item.Pid == pid
}

// Set ProcessingNode and Pid on this DPNWorkItem.
func (item *DPNWorkItem) SetNodeAndPid() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "hostname?"
	}
	item.ProcessingNode = &hostname
	item.Pid = os.Getpid()
}

// Clear ProcessingNode and Pid on this DPNWorkItem.
func (item *DPNWorkItem) ClearNodeAndPid() {
	item.ProcessingNode = nil
	item.Pid = 0
}
