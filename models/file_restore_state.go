package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// FileRestoreState stores information about the state of a file restoration
// operation. This entire structure will be converted to JSON and saved
// as a WorkItemState object in Pharos.
type FileRestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
	// GenericFile is the file we're going to restore. We don't
	// serialize this. We fetch it fresh from Pharos each time.
	GenericFile *GenericFile `json:"-"`
	// IntellectualObject is the object to which the file belongs.
	IntellectualObject *IntellectualObject `json:"-"`
	// RestoreSummary contains information about the restore operation,
	// such as when it started and completed, whether there were errors,
	// etc.
	RestoreSummary *WorkSummary
	// RestoredToUrl is a URL that points to the copy of this bag
	// in the depositor's S3 restoration bucket.
	RestoredToURL string
	// CopiedToRestorationAt is a timestamp describing when the
	// reassembled bag was copied to the depositor's S3 restoration
	// bucket.
	CopiedToRestorationAt time.Time
}

// NewFileRestoreState creates a new FileRestoreState object
// with empty RestoreSummary.
func NewFileRestoreState(message *nsq.Message) *FileRestoreState {
	return &FileRestoreState{
		NSQMessage:     message,
		RestoreSummary: NewWorkSummary(),
	}
}
