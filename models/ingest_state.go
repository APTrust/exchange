package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

type IngestState struct {
	NSQMessage     *nsq.Message
	WorkItem       *WorkItem
	WorkItemState  *WorkItemState
	IngestManifest *IngestManifest
}

// Tell NSQ we're still working on this item. NSQMessage will be nil if
// we're doing one-off testing (see RunWithoutNSQ).
func (ingestState *IngestState) TouchNSQ() {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Touch()
	}
}

// Tell NSQ we're done with this message.
func (ingestState *IngestState) FinishNSQ() {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Finish()
	}
}

// Requeue this item.
func (ingestState *IngestState) RequeueNSQ(milliseconds int) {
	if ingestState.NSQMessage != nil {
		ingestState.NSQMessage.Requeue(time.Duration(milliseconds) * time.Millisecond)
	}
}
