package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// DeleteState stores information about the state of a file deletion
// operation.
type DeleteState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
	// GenericFile is the file to be deleted.
	GenericFile *GenericFile `json:"-"`
	// DeleteSummary contains information about the outcome of the
	// attempt to delete the file.
	DeleteSummary *WorkSummary
	// DeletedFromPrimaryAt is a timestamp describing when the file
	// was deleted from primary storage (S3).
	DeletedFromPrimaryAt time.Time
	// DeletedFromSecondaryAt is a timestamp describing when the file
	// was deleted from secondary storage (Glacier).
	DeletedFromSecondaryAt time.Time
}

// NewDeleteState creates a new DeleteState object with an empty
// DeleteSummary.
func NewDeleteState(message *nsq.Message) *DeleteState {
	return &DeleteState{
		NSQMessage:    message,
		DeleteSummary: NewWorkSummary(),
	}
}
