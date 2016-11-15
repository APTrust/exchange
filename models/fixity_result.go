package models

import (
	"fmt"
	"strings"
)

// FixityResult descibes the results of fetching a file from S3
// and verification of the file's sha256 checksum.
type FixityResult struct {

	// The generic file we're going to look at.
	// This file is sitting somewhere on S3.
	GenericFile *GenericFile

	// Does the file exist in S3?
	S3FileExists bool

	// The sha256 sum we calculated after downloading
	// the file.
	Sha256 string

	// Information about the result of this operation.
	WorkSummary *WorkSummary
}

func NewFixityResult(gf *GenericFile) *FixityResult {
	return &FixityResult{
		GenericFile:  gf,
		S3FileExists: true,
		WorkSummary:  NewWorkSummary(),
	}
}

// Returns the name of the S3 bucket and key for the GenericFile.
func (result *FixityResult) BucketAndKey() (string, string, error) {
	parts := strings.Split(result.GenericFile.URI, "/")
	length := len(parts)
	if length < 4 {
		// This error is fatal, so don't retry.
		result.WorkSummary.AddError("GenericFile URI '%s' is invalid", result.GenericFile.URI)
		result.WorkSummary.Retry = false
		return "", "", fmt.Errorf(result.WorkSummary.FirstError())
	}
	bucket := parts[length-2]
	key := parts[length-1]
	return bucket, key, nil
}

// Returns true if result.Sha256 was set.
func (result *FixityResult) GotDigestFromPreservationFile() bool {
	return result.Sha256 != ""
}

// Returns true if the underlying GenericFile includes a SHA256 checksum.
func (result *FixityResult) GenericFileHasDigest() bool {
	return result.FedoraSha256() != ""
}

// Returns the SHA256 checksum that Fedora has on record.
func (result *FixityResult) FedoraSha256() string {
	checksum := result.GenericFile.GetChecksumByAlgorithm("sha256")
	if checksum == nil {
		return ""
	}
	return checksum.Digest
}

// Returns true if we have all the data we need to compare the
// existing checksum with the checksum of the S3 file.
func (result *FixityResult) FixityCheckPossible() bool {
	return result.GotDigestFromPreservationFile() && result.GenericFileHasDigest()
}

// Returns true if the sha256 sum we calculated for this file
// matches the sha256 sum recorded in Fedora.
func (result *FixityResult) Sha256Matches() (bool, error) {
	if result.FixityCheckPossible() == false {
		return false, fmt.Errorf("Fixity check is not possible because one or more checksums are not available.")
	}
	return result.FedoraSha256() == result.Sha256, nil
}
