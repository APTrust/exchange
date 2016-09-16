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
		manifest.CleanupResult.HasErrors())
}

func (manifest *IngestManifest) HasFatalErrors() (bool) {
	return (manifest.FetchResult.ErrorIsFatal ||
		manifest.UntarResult.ErrorIsFatal ||
		manifest.ValidateResult.ErrorIsFatal ||
		manifest.StoreResult.ErrorIsFatal ||
		manifest.RecordResult.ErrorIsFatal ||
		manifest.CleanupResult.ErrorIsFatal)
}

func (manifest *IngestManifest) AllErrorsAsString() (string) {
	errors := []string {
		manifest.FetchResult.AllErrorsAsString(),
		manifest.UntarResult.AllErrorsAsString(),
		manifest.ValidateResult.AllErrorsAsString(),
		manifest.StoreResult.AllErrorsAsString(),
		manifest.RecordResult.AllErrorsAsString(),
		manifest.CleanupResult.AllErrorsAsString(),
	}
	allErrors := ""
	for _, err := range errors {
		if err != "" {
			allErrors += err + "\n"
		}
	}
	return allErrors
}
