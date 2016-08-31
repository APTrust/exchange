package models

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"github.com/op/go-logging"
	"os"
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
	InstitutionId          int                  `json:"institution_id"`
	User                   string               `json:"user"`
	Date                   time.Time            `json:"date"`
	Note                   string               `json:"note"`
	Action                 string               `json:"action"`
	Stage                  string               `json:"stage"`
	Status                 string               `json:"status"`
	Outcome                string               `json:"outcome"`
	Retry                  bool                 `json:"retry"`
	// TODO: Change to binary, and possibly move.
	State                  string               `json:"state"`
	Node                   string               `json:"node"`
	Pid                    int                  `json:"pid"`
	NeedsAdminReview       bool                 `json:"needs_admin_review"`
	// QueuedAt is a nullable DateTime in the Rails app,
	// so it has to be a pointer here.
	QueuedAt               *time.Time           `json:"queued_at"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
}

// Convert WorkItem to JSON, omitting id and other attributes that
// Rails won't permit. For internal use, json.Marshal() works fine.
func (item *WorkItem) SerializeForPharos() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":                    item.Name,
		"bucket":                  item.Bucket,
		"etag":                    item.ETag,
		"bag_date":                item.BagDate,
		"institution_id":          item.InstitutionId,
		"object_identifier":       item.ObjectIdentifier,
		"generic_file_identifier": item.GenericFileIdentifier,
		"date":                    item.Date,
		"note":                    item.Note,
		"action":                  item.Action,
		"stage":                   item.Stage,
		"status":                  item.Status,
		"outcome":                 item.Outcome,
		"retry":                   item.Retry,
		"state":                   item.State,
		"node":                    item.Node,
		"pid":                     item.Pid,
		"needs_admin_review":      item.NeedsAdminReview,
		"queued_at":               item.QueuedAt,
	})
}

// Returns true if an object's files have been stored in S3 preservation bucket.
func (item *WorkItem) HasBeenStored() (bool) {
	if item.Action == constants.ActionIngest {
		return item.Stage == constants.StageRecord ||
			item.Stage == constants.StageCleanup ||
			item.Stage == constants.StageResolve ||
			(item.Stage == constants.StageStore && item.Status == constants.StatusPending)
	} else {
		return true
	}
}

func (item *WorkItem) IsStoring() (bool) {
	return item.Action == constants.ActionIngest &&
		item.Stage == constants.StageStore &&
		item.Status == constants.StatusStarted
}

// Returns true if we should try to ingest this item.
func (item *WorkItem) ShouldTryIngest() (bool) {
	return item.HasBeenStored() == false && item.IsStoring() == false && item.Retry == true
}

// Returns true if the WorkItem records include a delete
// request that has not been completed.
func HasPendingDeleteRequest(workItems []*WorkItem) (bool) {
	for _, record := range workItems {
		if record.Action == constants.ActionDelete &&
			(record.Status == constants.StatusStarted || record.Status == constants.StatusPending) {
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
			(record.Status == constants.StatusStarted || record.Status == constants.StatusPending) {
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
			(record.Status == constants.StatusStarted || record.Status == constants.StatusPending) {
			return true
		}
	}
	return false
}

// Set state, node and pid on WorkItem.
func (item *WorkItem) SetNodePidState(object interface{}, logger *logging.Logger) {
	jsonBytes, err := json.Marshal(object)
	jsonData := ""
	if err != nil {
		if logger != nil {
			logger.Error(err.Error())
		}
	} else {
		jsonData = string(jsonBytes)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "hostname?"
	}
	item.Node = hostname
	item.Pid = os.Getpid()
	item.State = jsonData
}
