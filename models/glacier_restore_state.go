package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

type GlacierRestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
	// WorkSummary contains information about whether/when
	// we requested this object(s) be restored from Glacier.
	WorkSummary *WorkSummary
	// Requests are the requests we've made (or need to make)
	// to Glacier to retrieve the objects we need to retrieve.
	Requests []*GlacierRestoreRequest
}

func NewGlacierRestoreState(message *nsq.Message, workItem *WorkItem) *GlacierRestoreState {
	return &GlacierRestoreState{
		NSQMessage:  message,
		WorkItem:    workItem,
		WorkSummary: NewWorkSummary(),
		Requests:    make([]*GlacierRestoreRequest, 0),
	}
}

func (state *GlacierRestoreState) FindRequest(gfIdentifier string) *GlacierRestoreRequest {
	var request *GlacierRestoreRequest
	if state.Requests != nil {
		for _, req := range state.Requests {
			if req.GenericFileIdentifier == gfIdentifier {
				request = req
				break
			}
		}
	}
	return request
}

type GlacierRestoreRequest struct {
	// GenericFileIdentifier is the identifier of the generic
	// file we want to restore.
	GenericFileIdentifier string
	// GlacierBucket is the bucket that contains the item
	// we want to restore.
	GlacierBucket string
	// GlacierKey is the key we want to restore
	// (usually a UUID, for APTrust).
	GlacierKey string
	// RequestAccepted indicates whether Glacier accepted
	// our request to restore this object.
	RequestAccepted bool
	// RequestedAt is the timestamp of the last request to
	// restore this object.
	RequestedAt time.Time
	// EstimatedDeletionFromS3 describes approximately when
	// this item should be available at the RestorationURL.
	// This time can vary, depending on what level of Glacier
	// retrieval service we're using. Using the standard service
	// level, this should be about four hours after RequestedAt,
	// if the requests succeeded.
	EstimatedDeletionFromS3 time.Time
	// SomeoneElseRequested will be true if apt_glacier_restore
	// thinks someone else requested retrieval of the object.
	// If this is true, EstimatedDeletionFromS3 may not be
	// reliable, because we don't know when the retrieval
	// request occurred, or with what parameters.
	SomeoneElseRequested bool
}
