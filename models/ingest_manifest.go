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

func (manifest *IngestManifest) HasErrors() (bool) {
	return (manifest.FetchResult.HasErrors() ||
		manifest.UntarResult.HasErrors() ||
		manifest.ValidateResult.HasErrors() ||
		manifest.StoreResult.HasErrors() ||
		manifest.RecordResult.HasErrors() ||
		manifest.ReplicateResult.HasErrors() ||
		manifest.CleanupResult.HasErrors())
}

func (manifest *IngestManifest) HasFatalErrors() (bool) {
	return (manifest.FetchResult.ErrorIsFatal ||
		manifest.UntarResult.ErrorIsFatal ||
		manifest.ValidateResult.ErrorIsFatal ||
		manifest.StoreResult.ErrorIsFatal ||
		manifest.RecordResult.ErrorIsFatal ||
		manifest.ReplicateResult.ErrorIsFatal ||
		manifest.CleanupResult.ErrorIsFatal)
}
