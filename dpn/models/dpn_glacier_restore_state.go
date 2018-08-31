package models

import (
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"time"
)

// DPNGlacierRestoreState holds information about the state of the Glacier
// restore process.
type DPNGlacierRestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// DPNWorkItem is the DPNWorkItem being processed. This object
	// comes from Pharos.
	DPNWorkItem *apt_models.DPNWorkItem
	// DPNBag is the bag we're restoring. This object comes from
	// the DPN REST server.
	DPNBag *DPNBag
	// GlacierBucket is the bucket that contains the item
	// we want to restore.
	GlacierBucket string
	// GlacierKey is the key we want to restore (the DPN Bag UUID).
	GlacierKey string
	// RequestAccepted indicates whether Glacier accepted
	// our request to restore this object. This does not mean
	// the request is complete. It can take several hours for
	// AWS to push the file from Glacier to S3. Check the
	// property IsAvailableInS3 to see if AWS has actually
	// completed the request.
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
	// IsAvailableInS3 describes whether the file has been
	// made available in S3 for download, a process which typically
	// takes 3-5 hours. If RequestAccepted is true and IsAvailableInS3
	// is false, then the request is still in process.
	IsAvailableInS3 bool
}
