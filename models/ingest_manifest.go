package models

import (
	"github.com/APTrust/exchange/util/fileutil"
	"os"
)

type IngestManifest struct {
	WorkItemId int
	// TODO: Get rid of bucket, key, and etag, since they're in WorkItem
	S3Bucket       string
	S3Key          string
	ETag           string
	BagPath        string
	DBPath         string
	FetchResult    *WorkSummary
	UntarResult    *WorkSummary
	ValidateResult *WorkSummary
	StoreResult    *WorkSummary
	RecordResult   *WorkSummary
	CleanupResult  *WorkSummary
	Object         *IntellectualObject
}

func NewIngestManifest() *IngestManifest {
	return &IngestManifest{
		FetchResult:    NewWorkSummary(),
		UntarResult:    NewWorkSummary(),
		ValidateResult: NewWorkSummary(),
		StoreResult:    NewWorkSummary(),
		RecordResult:   NewWorkSummary(),
		CleanupResult:  NewWorkSummary(),
		Object:         NewIntellectualObject(),
	}
}

// TODO: Write method to get first error, all errors.

func (manifest *IngestManifest) HasErrors() bool {
	return (manifest.FetchResult.HasErrors() ||
		manifest.UntarResult.HasErrors() ||
		manifest.ValidateResult.HasErrors() ||
		manifest.StoreResult.HasErrors() ||
		manifest.RecordResult.HasErrors() ||
		manifest.CleanupResult.HasErrors())
}

func (manifest *IngestManifest) HasFatalErrors() bool {
	return (manifest.FetchResult.ErrorIsFatal ||
		manifest.UntarResult.ErrorIsFatal ||
		manifest.ValidateResult.ErrorIsFatal ||
		manifest.StoreResult.ErrorIsFatal ||
		manifest.RecordResult.ErrorIsFatal ||
		manifest.CleanupResult.ErrorIsFatal)
}

func (manifest *IngestManifest) AllErrorsAsString() string {
	errors := []string{
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

// ClearAllErrors clears all of the errors on all of the WorkSummaries.
func (manifest *IngestManifest) ClearAllErrors() {
	manifest.FetchResult.ClearErrors()
	manifest.UntarResult.ClearErrors()
	manifest.ValidateResult.ClearErrors()
	manifest.StoreResult.ClearErrors()
	manifest.RecordResult.ClearErrors()
	manifest.CleanupResult.ClearErrors()
}

// BagIsOnDisk returns true if the bag (tar file) exists on disk.
func (manifest *IngestManifest) BagIsOnDisk() bool {
	return manifest.BagPath != "" && fileutil.FileExists(manifest.BagPath)
}

// DBExists returns true if the Bolt DB (.valdb file) exists on disk.
func (manifest *IngestManifest) DBExists() bool {
	return manifest.DBPath != "" && fileutil.FileExists(manifest.DBPath)
}

// SizeOfBagOnDisk returns the size, in bytes, of the bag on disk.
// This will return an error if the bag does not exist, or if it is
// a directory or is inaccessible.
func (manifest *IngestManifest) SizeOfBagOnDisk() (int64, error) {
	stat, err := os.Stat(manifest.BagPath)
	if err != nil {
		return int64(-1), err
	}
	return stat.Size(), nil
}

// BagHasBeenValidated returns true if the bag has already been validated.
func (manifest *IngestManifest) BagHasBeenValidated() bool {
	return (manifest.ValidateResult.Attempted == true &&
		manifest.ValidateResult.Finished() == true &&
		manifest.ValidateResult.HasErrors() == false)
}
