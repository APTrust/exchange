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
	// RestoreSummary contains information about the outcome
	// of this attempt to restore a bag.
	ValidateSummary *WorkSummary
	// LocalBagDir is the absolute path to the untarred bag. We'll be
	// assembling the bag contents in this directory.
	RestoreSummary *WorkSummary
	// ValidateSummary contains validation information about the
	// bag that we have assembled and tarred. The bag must be valid
	// before we copy it to the restoration bucket.
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
		RestoreSummary:  NewWorkSummary(),
	}
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
