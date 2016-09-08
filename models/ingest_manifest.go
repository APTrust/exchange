package models

import (
	"github.com/nsqio/go-nsq"
)

type IngestManifest struct {
	NSQMessage         *nsq.Message  `json:"-"`
	WorkItemId         int
	S3Bucket           string
	S3Key              string
	ETag               string
	Fetch              *WorkSummary
	Untar              *WorkSummary
	Validate           *WorkSummary
	Store              *WorkSummary
	Record             *WorkSummary
	Replicate          *WorkSummary
	Cleanup            *WorkSummary
	Object             *IntellectualObject
}

func NewIngestManifest() (*IngestManifest) {
	return &IngestManifest{
		Fetch: NewWorkSummary(),
		Untar: NewWorkSummary(),
		Validate: NewWorkSummary(),
		Store: NewWorkSummary(),
		Record: NewWorkSummary(),
		Replicate: NewWorkSummary(),
		Cleanup: NewWorkSummary(),
		Object: NewIntellectualObject(),
	}
}

// TODO: Write method to get first error, all errors.
