package models

import (
	"encoding/json"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"time"
)

// DPNRetrievalManifest contains information about the retrieval and
// postprocessing of a DPN bag for fixity checking or restoration.
type DPNRetrievalManifest struct {
	// NsqMessage is the NSQ message being processed to fulfill
	// this DPN fixity check request.
	NsqMessage *nsq.Message `json:"-"`

	// DPNWorkItem is the DPNWorkItem in Pharos that contains
	// information about this fixity task.
	DPNWorkItem *apt_models.DPNWorkItem

	// DPNBag is the bag we'll whose fixity we're checking. This
	// comes from the DPN REST server.
	DPNBag *DPNBag

	// TaskType describes whether this retrieval is for a fixity
	// check or a restoration. Value should be one of:
	// constants.ActionFixityCheck or constants.ActionRestore
	TaskType string

	// GlacierBucket is the bucket that contains the item
	// we want to restore.
	GlacierBucket string

	// GlacierKey is the name of this item in the Glacier bucket.
	// It should be the bag UUID with a ".tar" extension.
	GlacierKey string

	// RequestedFromGlacierAt is the timestamp of the last request to
	// Glacier to restore this object. That request may not have been
	// accepted. See GlacierRequestAccepted
	RequestedFromGlacierAt time.Time

	// GlacierRequestAccepted indicates whether Glacier accepted
	// our request to restore this object. This does not mean
	// the request is complete. It can take several hours for
	// AWS to push the file from Glacier to S3. Check the
	// property IsAvailableInS3 to see if AWS has actually
	// completed the request.
	GlacierRequestAccepted bool

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

	// LocalPath is the path to the downloaded file on the local
	// file system. This will be empty until we have actually
	// downloaded the file from S3.
	LocalPath string

	// RestorationURL is the URL from which the depositor can
	// retrieve the bag.
	RestorationURL string

	// S3Bucket is the bucket into which the item will be
	// restored from Glacier. Once here, the item can be
	// copied to a local volume for validation and fixity.
	S3Bucket string

	// ExpectedFixityValue is the SHA-256 digest that was calculated
	// for this bag's tagmanifest-sha256.txt file when the bag was
	// first ingested by the Ingest Node. This value comes from the
	// DPN REST server.
	ExpectedFixityValue string

	// ActualFixityValue is the SHA-256 digest that the worker
	// calculated for our stored copy of the bag's
	// tagmanifest-sha256.txt file.
	ActualFixityValue string

	// GlacierRestoreSummary is a summary of attempts to restore
	// the bag from Glacier to S3.
	GlacierRestoreSummary *apt_models.WorkSummary

	// LocalCopySummary is a summary attempts to copy the bag from
	// S3 (after Glacier restoration) to local storage.
	LocalCopySummary *apt_models.WorkSummary

	// ValidationSummary is a summary of attempts to validate the
	// bag. Because validation is IO-intensive and requires a lot
	// of random access, we do this locally, after the bag has been
	// copied to LocalPath. DPN rules say that a fixity check must:
	// 1) Validate the entire bag, 2) calculate the sha-256 digest
	// of the tagmanifest-sha256.txt file, and 3) record a new fixity
	// record in the DPN registry with the calculated digest.
	ValidationSummary *apt_models.WorkSummary

	// RecordSummary contains information about the worker's attempt
	// to record the result of the fixity check in the local DPN
	// registry (our node's instance of the DPN REST server).
	RecordSummary *apt_models.WorkSummary

	// FixityCheck is the FixityCheck record saved the DPN after
	// the worker finishes validating a bag and calculating the
	// sha256 checksum of its tagmanifest-sha256.txt file.
	// This property will be nil until the very last phase of
	// the fixity check process. For DPN bag restorations, this
	// property will remain nil throughout.
	FixityCheck *FixityCheck

	// FixityCheckSavedAt describes when we saved the FixityCheck
	// record to the local DPN REST server.
	FixityCheckSavedAt time.Time
}

func NewDPNRetrievalManifest(message *nsq.Message) *DPNRetrievalManifest {
	return &DPNRetrievalManifest{
		NsqMessage:            message,
		GlacierRestoreSummary: apt_models.NewWorkSummary(),
		LocalCopySummary:      apt_models.NewWorkSummary(),
		ValidationSummary:     apt_models.NewWorkSummary(),
		RecordSummary:         apt_models.NewWorkSummary(),
	}
}

func DPNRetrievalManifestFromJson(jsonString string) (*DPNRetrievalManifest, error) {
	manifest := &DPNRetrievalManifest{}
	err := json.Unmarshal([]byte(jsonString), manifest)
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func (manifest *DPNRetrievalManifest) ToJson() (string, error) {
	jsonStr, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return string(jsonStr), nil
}

func (manifest *DPNRetrievalManifest) GetSummary(name string) *apt_models.WorkSummary {
	if name == "GlacierRestoreSummary" {
		return manifest.GlacierRestoreSummary
	} else if name == "LocalCopySummary" {
		return manifest.LocalCopySummary
	} else if name == "ValidationSummary" {
		return manifest.ValidationSummary
	} else if name == "RecordSummary" {
		return manifest.RecordSummary
	}
	return nil
}

func (manifest *DPNRetrievalManifest) CheckCompletedAndFailed() bool {
	return (manifest.ExpectedFixityValue != "" &&
		manifest.ActualFixityValue != "" &&
		manifest.ExpectedFixityValue != manifest.ActualFixityValue)
}
