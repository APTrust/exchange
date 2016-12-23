package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// RestoreState stores information about the state of a bag restoration
// operation.
type RestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request.
	NSQMessage *nsq.Message
	// WorkItem is the Pharos WorkItem we're processing.
	WorkItem *WorkItem
	// IntellectualObject is the object we're restoring.
	IntellectualObject *IntellectualObject
	// RestoreSummary contains information about the outcome
	// of this attempt to restore a bag.
	RestoreSummary *WorkSummary
	// ValidateSummary contains validation information about the
	// bag that we have assembled and tarred. The bag must be valid
	// before we copy it to the restoration bucket.
	ValidateSummary *WorkSummary
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
