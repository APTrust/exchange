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
	// GenericFile is the generic file whose fixity we're going to check.
	// This file is sitting somewhere on S3.
	GenericFile *GenericFile
	// S3FileExists describes whether the GenericFile file exist in S3.
	S3FileExists bool
	// Sha256 contains sha256 digest we calculated after downloading
	// the file. This will be empty initially.
	Sha256 string
	// FixityCheckSumary contains information about the result of the
	// fixity check.
	FixityCheckSummary *WorkSummary
	// RecordSummary contains information about the result of the
	// attempt to record the fixity check PREMIS event in Pharos.
	RecordSummary *WorkSummary
}

// NewFixityResult returns a new empty FixityResult object for the specified
// GenericFile.
//
// TODO: Change constructor to use nsq.Message?
func NewFixityResult(message *nsq.Message) *FixityResult {
	return &FixityResult{
		NSQMessage:         message,
		S3FileExists:       true,
		FixityCheckSummary: NewWorkSummary(),
		RecordSummary:      NewWorkSummary(),
	}
}

// BucketAndKey returns the name of the S3 bucket and key for the GenericFile.
func (result *FixityResult) BucketAndKey() (string, string, error) {
	if result.GenericFile == nil {
		return "", "", fmt.Errorf("FixityResult.GenericFile is nil")
	}
	parts := strings.Split(result.GenericFile.URI, "/")
	length := len(parts)
	if length < 4 {
		return "", "", fmt.Errorf("GenericFile URI '%s' is invalid", result.GenericFile.URI)
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
	if result.GenericFile == nil {
		return ""
	}
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
