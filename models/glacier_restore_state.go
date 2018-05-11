package models

import (
	"time"
)

type GlacierRestoreState struct {
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
	// RequestSummary contains information about whether/when
	// we requested this object(s) be restored from Glacier.
	RequestSummary *WorkSummary
	// Requests are the requests we've made (or need to make)
	// to Glacier to retrieve the objects we need to retrieve.
	Requests []GlacierRestoreRequest
}

// TODO: Implement functions to add and find restore requests.

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
	// AttemptNumber is the number of times we've requested
	// the item be restored.
	AttemptNumber int
	// ObjectSize is the size (in bytes) of the object we
	// want to restore.
	ObjectSize int64
	// RequestAccepted indicates whether Glacier accepted
	// our request to restore this object.
	RequestAccepted bool
	// RequestedAt is the timestamp of the last request to
	// restore this object.
	RequestedAt time.Time
	// AcceptedAt is the timestamp describing when Glacier
	// accepted the restore request.
	AcceptedAt time.Time
	// EstimatedAvailabilityTime describes approximately when
	// this item should be available at the RestorationURL.
	// This time can vary, depending on what level of Glacier
	// retrieval service we're using. Using the standard service
	// level, this should be about four hours after RequestedAt,
	// if the requests succeeded.
	EstimatedAvailabilityTime time.Time
	// RestorationURL is the URL in S3 where this object will
	// be available once it's been restored.
	RestorationURL string
	// DaysAvailable is the number of days after restoration that
	// this item will remain in the S3 bucket.
	DaysAvailable int
	// ObjectMetadata is the metadata attached to the Glacier
	// object at the time we reqested its retrieval.
	ObjectMetadata map[string]string
}
