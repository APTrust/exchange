package models

import (
	"time"
)

// QueueItem describes a single replication, restore or ingest
// request encountered by the dpn_queue application, which checks
// for and queues new DPN requests
type QueueItem struct {
	// Identifier is a UUID in the case of ReplicationTransfers and
	// RestoreTransfers, or an IntellectualObject.Identifier in the
	// case of Ingest requests. It identifies the subject of the request.
	Identifier string
	// The Id of the DPNWorkItem that corresponds to this replication or
	// restore requests. For ingest requests, it's the Id of the WorkItem.
	ItemId int
	// QueuedAt is the time at which this item was queued.
	QueuedAt time.Time
}

// NewQueueItem creates a new QueueItem with the specified identifier.
func NewQueueItem(identifier string) *QueueItem {
	return &QueueItem{Identifier: identifier}
}

// QueueResult describes the result of one run of workers/dpn_queue.
// This info is dumped into a JSON log by dpn_queue.
type QueueResult struct {
	// StartTime is the time at which dpn_queue started.
	StartTime time.Time
	// EndTime is the time at which dpn_queue finished its processing.
	EndTime time.Time
	// Replications is a list of ReplicationTransfer requests encountered
	// during this run.
	Replications []*QueueItem
	// Restores is a list of RestoreTransfer requests encountered
	// during this run.
	Restores []*QueueItem
	// Ingests is a list of ingest requests encountered during this run.
	Ingests []*QueueItem
	// Errors is a list of errors that occurred during processing.
	Errors []string
}

// NewQueueResult returns a new QueueResult object.
func NewQueueResult() *QueueResult {
	return &QueueResult{
		Replications: make([]*QueueItem, 0),
		Restores:     make([]*QueueItem, 0),
		Ingests:      make([]*QueueItem, 0),
		Errors:       make([]string, 0),
	}
}

// AddError adds an error message to the Errors list.
func (result *QueueResult) AddError(errMsg string) {
	result.Errors = append(result.Errors, errMsg)
}

// HasErrors returns true if there are any errors in this queue result.
func (result *QueueResult) HasErrors() bool {
	return len(result.Errors) > 0
}

// AddReplication adds a QueueItem to the Replications list.
func (result *QueueResult) AddReplication(item *QueueItem) {
	result.Replications = append(result.Replications, item)
}

// AddRestore adds a QueueItem to the Restores list.
func (result *QueueResult) AddRestore(item *QueueItem) {
	result.Restores = append(result.Restores, item)
}

// AddIngest adds a QueueItem to the Ingests list.
func (result *QueueResult) AddIngest(item *QueueItem) {
	result.Ingests = append(result.Ingests, item)
}

// FindReplication returns the Replication QueueItem with the specified
// identifier, or nil.
func (result *QueueResult) FindReplication(identifier string) *QueueItem {
	for _, item := range result.Replications {
		if item.Identifier == identifier {
			return item
		}
	}
	return nil
}

// FindRestore returns the Restore QueueItem with the specified
// identifier, or nil.
func (result *QueueResult) FindRestore(identifier string) *QueueItem {
	for _, item := range result.Restores {
		if item.Identifier == identifier {
			return item
		}
	}
	return nil
}

// FindIngest returns the Ingest QueueItem with the specified
// identifier, or nil.
func (result *QueueResult) FindIngest(identifier string) *QueueItem {
	for _, item := range result.Ingests {
		if item.Identifier == identifier {
			return item
		}
	}
	return nil
}
