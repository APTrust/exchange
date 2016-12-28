package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// RestoreState stores information about the state of a bag restoration
// operation. This entire structure will be converted to JSON and saved
// as a WorkItemState object in Pharos.
type RestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json"-"`
	// IntellectualObject is the object we're restoring. Not serialized
	// because if the object has thousands of files, the serialization is
	// huge.
	IntellectualObject *IntellectualObject `json"-"`
	// PackageSummary contains information about the outcome of the
	// attempt to reassemble this bag for restoration.
	PackageSummary *WorkSummary
	// ValidateSummary contains information about the outcome
	// of validating this newly reassembled bag. We must validate
	// it before sending it to the restoration bucket.
	ValidateSummary *WorkSummary
	// CopySummary contains information about the outcome of the
	// attempt to copy the tarred bag to the depositor's restoration
	// bucket.
	CopySummary *WorkSummary
	// RecordSummary contains information about the outcome of
	// attempts to record the restoration event and the completion
	// of the WorkItem in Pharos.
	RecordSummary *WorkSummary
	// LocalBagDir is the absolute path to the untarred bag. We'll be
	// assembling the bag contents in this directory.
	LocalBagDir string
	// LocalTarFile is the absolute path the tarred version of this
	// bag. The local tar file will not exist until the bag has been
	// fully assembled and tarred.
	LocalTarFile string
	// RestoredToUrl is a URL that points to the copy of this bag
	// in the depositor's S3 restoration bucket.
	RestoredToUrl string
	// CopiedToRestorationAt is a timestamp describing when the
	// reassembled bag was copied to the depositor's S3 restoration
	// bucket.
	CopiedToRestorationAt time.Time
}

// NewRestoreState creates a new RestoreState object with empty
// PackageSummary, RestoreSummary, and ValidationSummary.
func NewRestoreState(message *nsq.Message) *RestoreState {
	return &RestoreState{
		NSQMessage:      message,
		PackageSummary:  NewWorkSummary(),
		ValidateSummary: NewWorkSummary(),
		RecordSummary:   NewWorkSummary(),
		CopySummary:     NewWorkSummary(),
	}
}

// HasErrors returns true if any of the work summaries have errors.
func (restoreState *RestoreState) HasErrors() bool {
	return restoreState.PackageSummary.HasErrors() ||
		restoreState.ValidateSummary.HasErrors() ||
		restoreState.RecordSummary.HasErrors() ||
		restoreState.CopySummary.HasErrors()
}

// HasFatalErrors returns true if any of the work summaries have
// a fatal error.
func (restoreState *RestoreState) HasFatalErrors() bool {
	return (restoreState.PackageSummary.ErrorIsFatal ||
		restoreState.ValidateSummary.ErrorIsFatal ||
		restoreState.RecordSummary.ErrorIsFatal ||
		restoreState.CopySummary.ErrorIsFatal)
}

// AllErrorsAsString returns all error messages from all work summaries
// as a single string.
func (restoreState *RestoreState) AllErrorsAsString() string {
	errors := []string{
		restoreState.PackageSummary.AllErrorsAsString(),
		restoreState.ValidateSummary.AllErrorsAsString(),
		restoreState.RecordSummary.AllErrorsAsString(),
		restoreState.CopySummary.AllErrorsAsString(),
	}
	allErrors := ""
	for _, err := range errors {
		if err != "" {
			allErrors += err + "\n"
		}
	}
	return allErrors
}

// TouchNSQ tells NSQ we're still working on this item.
func (restoreState *RestoreState) TouchNSQ() {
	if restoreState.NSQMessage != nil {
		restoreState.NSQMessage.Touch()
	}
}

// FinishNSQ tells NSQ we're done with this message.
func (restoreState *RestoreState) FinishNSQ() {
	if restoreState.NSQMessage != nil {
		restoreState.NSQMessage.Finish()
	}
}

// RequeueNSQ tells NSQ to give this item to give this item to another
// worker (or perhaps the same worker) after a delay of at least the
// specified number of milliseconds.
func (restoreState *RestoreState) RequeueNSQ(milliseconds int) {
	if restoreState.NSQMessage != nil {
		restoreState.NSQMessage.Requeue(time.Duration(milliseconds) * time.Millisecond)
	}
}
