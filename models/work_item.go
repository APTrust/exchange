package models

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"time"
)


// WorkItem contains summary information describing
// the status of a bag in process. This data goes to Fluctus,
// so that APTrust partners can see which of their bags have
// been processed successfully, and why failed bags failed.
// See http://bit.ly/1pf7qxD for details.
//
// Type may have one of the following values: Ingest, Delete,
// Restore
//
// Stage may have one of the following values: Receive (bag was
// uploaded by partner into receiving bucket), Fetch (fetch
// tarred bag file from S3 receiving bucket), Unpack (unpack
// the tarred bag), Validate (make sure all data files are present,
// checksums are correct, required tags are present), Store (copy
// generic files to permanent S3 bucket for archiving), Record
// (save record of intellectual object, generic files and events
// to Fedora).
//
// Status may have one of the following values: Pending,
// Success, Failed.
type WorkItem struct {
	Id                     int                  `json:"id"`
	ObjectIdentifier       string               `json:"object_identifier"`
	GenericFileIdentifier  string               `json:"generic_file_identifier"`
	Name                   string               `json:"name"`
	Bucket                 string               `json:"bucket"`
	ETag                   string               `json:"etag"`
	BagDate                time.Time            `json:"bag_date"`
	Institution            string               `json:"institution"`
	User                   string               `json:"user"`
	Date                   time.Time            `json:"date"`
	Note                   string               `json:"note"`
	Action                 constants.ActionType `json:"action"`
	Stage                  constants.StageType  `json:"stage"`
	Status                 constants.StatusType `json:"status"`
	Outcome                string               `json:"outcome"`
	Retry                  bool                 `json:"retry"`
	Reviewed               bool                 `json:"reviewed"`
}

// Convert WorkItem to JSON, omitting id, which Rails won't permit.
// For internal use, json.Marshal() works fine.
func (status *WorkItem) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":                    status.Name,
		"bucket":                  status.Bucket,
		"etag":                    status.ETag,
		"bag_date":                status.BagDate,
		"institution":             status.Institution,
		"object_identifier":       status.ObjectIdentifier,
		"generic_file_identifier": status.GenericFileIdentifier,
		"date":                    status.Date,
		"note":                    status.Note,
		"action":                  status.Action,
		"stage":                   status.Stage,
		"status":                  status.Status,
		"outcome":                 status.Outcome,
		"retry":                   status.Retry,
		"reviewed":                status.Reviewed,
	})
}

// Returns true if an object's files have been stored in S3 preservation bucket.
func (status *WorkItem) HasBeenStored() (bool) {
	if status.Action == constants.ActionIngest {
		return status.Stage == constants.StageRecord ||
			status.Stage == constants.StageCleanup ||
			status.Stage == constants.StageResolve ||
			(status.Stage == constants.StageStore &&
			status.Status == constants.StatusPending)
	} else {
		return true
	}
}

func (status *WorkItem) IsStoring() (bool) {
	return status.Action == constants.ActionIngest &&
		status.Stage == constants.StageStore &&
		status.Status == constants.StatusStarted
}

// Returns true if we should try to ingest this item.
func (status *WorkItem) ShouldTryIngest() (bool) {
	return status.HasBeenStored() == false && status.IsStoring() == false && status.Retry == true
}

// Returns true if the WorkItem records include a delete
// request that has not been completed.
func HasPendingDeleteRequest(workItems []*WorkItem) (bool) {
	for _, record := range workItems {
		if record.Action == constants.ActionDelete &&
			(record.Status == constants.StatusStarted ||
			record.Status == constants.StatusPending) {
			return true
		}
	}
	return false
}

// Returns true if the WorkItem records include a restore
// request that has not been completed.
func HasPendingRestoreRequest(workItems []*WorkItem) (bool) {
	for _, record := range workItems {
		if record.Action == constants.ActionRestore &&
			(record.Status == constants.StatusStarted ||
			record.Status == constants.StatusPending) {
			return true
		}
	}
	return false
}

// Returns true if the WorkItem records include an ingest
// request that has not been completed.
func HasPendingIngestRequest(workItems []*WorkItem) (bool) {
	for _, record := range workItems {
		if record.Action == constants.ActionIngest &&
			(record.Status == constants.StatusStarted ||
			record.Status == constants.StatusPending) {
			return true
		}
	}
	return false
}
