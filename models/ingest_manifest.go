package models

type IngestManifest struct {
	WorkItemId         int
	S3Bucket           string
	S3Key              string
	ETag               string
	FetchResult        *WorkSummary
	UntarResult        *WorkSummary
	ValidateResult     *WorkSummary
	StoreResult        *WorkSummary
	RecordResult       *WorkSummary
	ReplicateResult    *WorkSummary
	CleanupResult      *WorkSummary
	Object             *IntellectualObject
}

func NewIngestManifest() (*IngestManifest) {
	return &IngestManifest{
		FetchResult: NewWorkSummary(),
		UntarResult: NewWorkSummary(),
		ValidateResult: NewWorkSummary(),
		StoreResult: NewWorkSummary(),
		RecordResult: NewWorkSummary(),
		ReplicateResult: NewWorkSummary(),
		CleanupResult: NewWorkSummary(),
		Object: NewIntellectualObject(),
	}
}

// TODO: Write method to get first error, all errors.
