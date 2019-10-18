package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
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
	// Id is the unique identifier for this work item.
	Id int `json:"id"`
	// ObjectIdentifier is the identifier of the IntellectualObject
	// that is the subject of this WorkItem. E.g. "virginia.edu/bag1234".
	// ObjectIdentifier will be empty until an item is ingested.
	// ObjectIdentifier should be calculated as institution identifier,
	// plus a slash, plus Name (see below) without the ".tar" extension.
	// E.g. "virginia.edu/bag1234"
	ObjectIdentifier string `json:"object_identifier"`
	// GenericFileIdentifier is the identifier of the GenericFile to
	// which this WorkItem pertains. This will be empty for WorkItems
	// that only make sense at the object level such as Ingest.
	// If GenericFileIdentifier is non-empty, it
	// means the work for this WorkItem is to be performed on the
	// GenericFile and not the object. For example, file deletion
	// and fixity checking are performed on GenericFiles, not
	// IntellectualObjects.
	GenericFileIdentifier string `json:"generic_file_identifier"`
	// Name is the name of the S3 key in the receiving bucket where
	// this object first appeared. It should match the bag name, minus
	// the institution prefix, with a ".tar" extension at the end.
	// For example, if the IntellectualObject is "virginia.edu/bag1234",
	// Name should be "bag1234.tar".
	Name string `json:"name"`
	// Bucket is the S3 receiving bucket to which this item was uploaded.
	Bucket string `json:"bucket"`
	// ETag is the S3 etag associated with this item. When a depositor
	// uploads a new version of an existing bag, you'll find a new
	// WorkItem record with the same Name and Bucket, but a different
	// ETag.
	ETag string `json:"etag"`
	// Size is the size, in bytes, of the tar file in the S3 receiving
	// bucket. This may not match the size of the restored bag because:
	// 1) on restoration, we add a sha256 manifest if it didn't exist
	// in the original bag, and 2) the depositor may have deleted some
	// files from the IntellectualObject before restoring the bag.
	Size int64 `json:"size"`
	// BagDate is the creation timestamp on the bag, as reported by S3.
	// This should be the date and time that the depositor created the
	// bag (not the date/time when S3 received it).
	BagDate time.Time `json:"bag_date"`
	// InstitutionId is the unique identifier of the institution to
	// whom this bag belongs.
	InstitutionId int `json:"institution_id"`
	// WorkItemStateId is the unique id of the WorkItemState record that
	// contains JSON data describing the last known state of processing
	// on this WorkItem. If processing has not started yet for this item,
	// it will have no WorkItemState.
	WorkItemStateId *int `json:"work_item_state_id"`
	// User is the email address of the user who requested this WorkItem.
	// For Ingest, this will always be the system user. For restoration,
	// deletion requests, it will be the email address of
	// the user who clicked the button or submitted the API request to
	// start the process.
	User string `json:"user"`
	// InstitutionalApprover is for deletions only and will be null for all
	// actions other than deletion. This is the email
	// address of the institutional admin who approved the deletion.
	// Exchange services should not process deletions where this field
	// is nil.
	InstitutionalApprover *string `json:"inst_approver"`
	// APTrustApprover is for bulk deletions only and will be null for
	// all actions other than deletion. This is the email
	// address of the APTrust admin who approved the deletion.
	APTrustApprover *string `json:"aptrust_approver"`
	// Date is the timestamp describing when some worker process last
	// touched this item.
	Date time.Time `json:"date"`
	// Note is a human-readable note about the status of this WorkItem.
	// This note is intended for users checking on the state of their
	// work items, so it should be descriptive.
	Note string `json:"note"`
	// Action is the action to be performed in this WorkItem. See
	// constants.ActionTypes.
	Action string `json:"action"`
	// Stage is the current stage of processing for this item. See.
	// constants.StageTypes.
	Stage string `json:"stage"`
	// StageStartedAt describes when processing started for the current
	// stage. If it's empty, processing for the current stage has not
	// begun.
	StageStartedAt *time.Time `json:"stage_started_at"`
	// Status is the status of this WorkItem. See the values in
	// constants.StatusTypes.
	Status string `json:"status"`
	// Outcome describes the outcome of a completed WorkItem. For example,
	// Success, Failure, Cancelled.
	Outcome string `json:"outcome"`
	// Retry indicates whether or not a failed or uncompleted WorkItem
	// should be retried. This will be set to false if processing resulted
	// in a fatal error (such as when trying to ingest an invalid bag) or
	// when processing has encountered too many transient errors (such as
	// network connection problems, lack of disk space, etc.). The threshold
	// for transient errors is defined by the MaxAttempts config setting for
	// each WorkerConfig. WorkItems that fail due to repeated transient errors
	// should be requeued by an administrator when the cause of the transient
	// error resolves. Part of the requeuing process involves setting Retry
	// back to true.
	Retry bool `json:"retry"`
	// Node is the hostname or IP address of the machine that is currently
	// processing this request. The worker process sets this field when it
	// begins processing and clears it when it's done. If a worker crashes,
	// you can identify orphaned WorkItems by the combination of non-empty
	// Node and StageStartedAt. You can check Pid as well, to see if it's
	// still running on Node.
	Node string `json:"node"`
	// Pid is the process id of the worker currently handling this WorkItem.
	// Workers set and clear Pid in the same way they set and clear Node.
	Pid int `json:"pid"`
	// NeedsAdminReview indicates whether an administrator needs to look into
	// this WorkItem. The worker process that attepts to fulfill this WorkItem
	// will set this to true when it encounters unexpected errors.
	NeedsAdminReview bool `json:"needs_admin_review"`
	// QueuedAt describes when this item was copied into NSQ. This is a nullable
	// DateTime in the Rails app, so it has to be a pointer here. When requeuing
	// an item, set this to nil and set Retry to true. Otherwise, apt_queue will
	// ignore it. QueuedAt exists to prevent apt_queue from adding items more than
	// once to an NSQ topic.
	QueuedAt *time.Time `json:"queued_at"`
	// CreatedAt is the Rails timestamp describing when this item was created.
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is the Rails timestamp describing when this item was updated.
	UpdatedAt time.Time `json:"updated_at"`
}

// Convert WorkItem to JSON, omitting id and other attributes that
// Rails won't permit. For internal use, json.Marshal() works fine.
func (item *WorkItem) SerializeForPharos() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":                    item.Name,
		"bucket":                  item.Bucket,
		"etag":                    item.ETag,
		"size":                    item.Size,
		"bag_date":                item.BagDate,
		"institution_id":          item.InstitutionId,
		"object_identifier":       item.ObjectIdentifier,
		"generic_file_identifier": item.GenericFileIdentifier,
		"date":                    item.Date,
		"note":                    item.Note,
		"action":                  item.Action,
		"stage":                   item.Stage,
		"stage_started_at":        item.StageStartedAt,
		"status":                  item.Status,
		"outcome":                 item.Outcome,
		"retry":                   item.Retry,
		"node":                    item.Node,
		"pid":                     item.Pid,
		"needs_admin_review":      item.NeedsAdminReview,
		"queued_at":               item.QueuedAt,
		"user":                    item.User,
		"inst_approver":           item.InstitutionalApprover,
		"aptrust_approver":        item.APTrustApprover,
	})
}

// Returns true if an object's files have been stored in S3 preservation bucket.
func (item *WorkItem) HasBeenStored() bool {
	if item.Action == constants.ActionIngest {
		return item.Stage == constants.StageRecord ||
			item.Stage == constants.StageCleanup ||
			item.Stage == constants.StageResolve ||
			(item.Stage == constants.StageStore && item.Status == constants.StatusPending)
	} else {
		return true
	}
}

func (item *WorkItem) IsStoring() bool {
	return item.Action == constants.ActionIngest &&
		item.Stage == constants.StageStore &&
		item.Status == constants.StatusStarted
}

// Returns true if we should try to ingest this item.
func (item *WorkItem) ShouldTryIngest() bool {
	return item.HasBeenStored() == false && item.IsStoring() == false && item.Retry == true
}

// Returns true if the WorkItem records include a delete
// request that has not been completed.
func HasPendingDeleteRequest(workItems []*WorkItem) bool {
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
func HasPendingRestoreRequest(workItems []*WorkItem) bool {
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
func HasPendingIngestRequest(workItems []*WorkItem) bool {
	for _, record := range workItems {
		if record.Action == constants.ActionIngest &&
			(record.Status == constants.StatusStarted || record.Status == constants.StatusPending) {
			return true
		}
	}
	return false
}

// Set state, node and pid on WorkItem.
func (item *WorkItem) SetNodeAndPid() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "hostname?"
	}
	item.Node = hostname
	item.Pid = os.Getpid()
}

// Returns true if this item is currently being processed
// by another worker.
func (item *WorkItem) BelongsToAnotherWorker() bool {
	if item.Node == "" {
		return false
	}
	hostname, _ := os.Hostname()
	return item.Node != hostname || item.Pid != os.Getpid()
}

// IsInProgress returns true if any worker is currently
// working on this item.
func (item *WorkItem) IsInProgress() bool {
	return item.Node != "" && item.Pid != 0
}

// IsPastIngest returns true if this item has already passed
// the ingest stage.
func (item *WorkItem) IsPastIngest() bool {
	return (item.Stage != constants.StageReceive &&
		item.Stage != constants.StageFetch &&
		item.Stage != constants.StageUnpack &&
		item.Stage != constants.StageValidate)
}

// MsgSkippingInProgress returns a message saying that a worker
// is skipping this item because it's already being processed.
func (item *WorkItem) MsgSkippingInProgress() string {
	return fmt.Sprintf("Marking NSQ message for WorkItem %d (%s) as finished "+
		"without doing any work, because this item is currently in process by "+
		"node %s, pid %d. WorkItem was last updated at %s.",
		item.Id, item.Name, item.Node, item.Pid, item.UpdatedAt)
}

// MsqPastIngest returns a message saying that a worker is skipping
// this item because it's past the ingest stage.
func (item *WorkItem) MsgPastIngest() string {
	return fmt.Sprintf("Marking NSQ Message for WorkItem %d (%s) as finished "+
		"without doing any work, because this item is already past the "+
		"ingest phase.", item.Id, item.Name)
}

// MsgAlreadyOnDisk returns a message saying the bag has already
// been downloaded to the local disk.
func (item *WorkItem) MsgAlreadyOnDisk() string {
	return fmt.Sprintf("Bag %s is already on disk and appears to be complete.", item.Name)
}

// MsgAlreadyValidated returns a message saying the bag has already
// been validated.
func (item *WorkItem) MsgAlreadyValidated() string {
	return fmt.Sprintf("Bag %s has already been validated. "+
		"Now it's going to the cleanup channel.", item.Name)
}

// MsgGoingToValidation returns a message saying this item is being
// put into the validation channel.
func (item *WorkItem) MsgGoingToValidation() string {
	return fmt.Sprintf("Bag %s is going into the validation channel.", item.Name)
}

// MsgGoingToFetch returns a message saying this item is being
// put into the fetch channel.
func (item *WorkItem) MsgGoingToFetch() string {
	return fmt.Sprintf("Bag %s is going into the fetch channel.", item.Name)
}
