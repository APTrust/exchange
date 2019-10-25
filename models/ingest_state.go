package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// IngestState stores information about the state of ingest operations
// for a single bag being ingested into APTrust. The ingest process involves
// a number of steps and worker processes. This state object is passed from
// one worker to the next, and accompanies the bag through every step of
// the process. If ingest fails, this object contains enough information to
// tell us why the ingest failed, where it failed, at which step it should be
// resumed, and whether there's anything (like partial files) that need to be
// cleaned up.
type IngestState struct {
	NSQMessage     *nsq.Message `json:"-"`
	WorkItem       *WorkItem
	WorkItemState  *WorkItemState
	IngestManifest *IngestManifest

	// RequeueDelay is the number of milliseconds NSQ should delay before
	// sending this message to a worker. We use this in apt_storer.go
	// when we have to requeue a high-resource bag (huge size or huge number
	// of files) for a later time when the worker is less busy.
	RequeueDelay int
}

// TouchNSQ tells NSQ we're still working on this item.
func (ingestState *IngestState) TouchNSQ() {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Touch()
	}
}

// FinishNSQ tells NSQ we're done with this message.
func (ingestState *IngestState) FinishNSQ() {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Finish()
	}
}

// RequeueNSQ tells NSQ to give this item to give this item to another
// worker (or perhaps the same worker) after a delay of at least the
// specified number of milliseconds.
func (ingestState *IngestState) RequeueNSQ(milliseconds int) {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Requeue(time.Duration(milliseconds) * time.Millisecond)
	}
}
