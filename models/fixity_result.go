package models

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"strings"
)

// FixityResult descibes the results of fetching a file from S3
// and verification of the file's sha256 checksum.
type FixityResult struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
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

// NewFixityResult returns a new empty FixityResult object for the specified
// GenericFile.
func NewFixityResult(gf *GenericFile) *FixityResult {
	return &FixityResult{
		GenericFile:  gf,
		S3FileExists: true,
		WorkSummary:  NewWorkSummary(),
	}
}

// BucketAndKey returns the name of the S3 bucket and key for the GenericFile.
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

// GotDigestFromPreservationFile returns true if result.Sha256 was set.
func (result *FixityResult) GotDigestFromPreservationFile() bool {
	return result.Sha256 != ""
}

// GenericFileHasDigest returns true if the underlying GenericFile
// includes a SHA256 checksum.
func (result *FixityResult) GenericFileHasDigest() bool {
	return result.FedoraSha256() != ""
}

// FedoraSha256 returns the SHA256 checksum that Fedora has on record.
func (result *FixityResult) FedoraSha256() string {
	checksum := result.GenericFile.GetChecksumByAlgorithm("sha256")
	if checksum == nil {
		return ""
	}
	return checksum.Digest
}

// FixityCheckPossible returns true if we have all the data we need to
// compare the existing checksum with the checksum of the S3 file.
func (result *FixityResult) FixityCheckPossible() bool {
	return result.GotDigestFromPreservationFile() && result.GenericFileHasDigest()
}

// Sha256Matches returns true if the sha256 sum we calculated for this
// file matches the sha256 sum recorded in Fedora.
func (result *FixityResult) Sha256Matches() (bool, error) {
	if result.FixityCheckPossible() == false {
		return false, fmt.Errorf("Fixity check is not possible because one or more checksums are not available.")
	}
	return result.FedoraSha256() == result.Sha256, nil
}
