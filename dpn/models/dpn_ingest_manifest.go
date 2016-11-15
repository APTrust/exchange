package models

import (
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
)

// DPNIngestManifest contains information about a bag being
// ingested from APTrust to DPN.
type DPNIngestManifest struct {
	// NsqMessage is the NSQ message being processed to fulfill
	// this ingest request.
	NsqMessage          *nsq.Message `json:"-"`

	// WorkItem is the WorkItem in Pharos that maintains
	// information about this ingest.
	WorkItem            *apt_models.WorkItem

	// IntellectualObject is the object we are pushing into DPN.
	IntellectualObject  *apt_models.IntellectualObject

	// LocalDir is the directory in which we are assembling the
	// contents of the DPN bag.
	LocalDir            string

	// LocalTarFile is the path the tarred DPN bag that we have
	// built, and which we will copy to long-term storage.
	LocalTarFile        string

	// StorageURL is the URL of the tarred bag that has been copied
	// into long-term storage.
	StorageURL          string

	// BagMd5Digest is the bag's md5 digest. We need this to copy to
	// Amazon S3/Glacier.
	BagMd5Digest        string

	// DPNBag is the DPNBag record we will create for the bag we're
	// sending into long-term storage. Once the bag is stored, we'll
	// POST this DPNBag record to our local DPN REST server.
	DPNBag             *DPNBag

	// PackageSummary describes what happened during the packaging
	// of this DPN bag. The process involves packing all of the
	// IntellectualObject's files into a DPN bag, whose structure
	// differs somewhat from the structure of an APTrust bag.
	PackageSummary     *apt_models.WorkSummary

	// ValidateSummary describes the result of running our validator
	// over the DPN bag we just created.
	ValidateSummary    *apt_models.WorkSummary

	// StoreSummary describes the result of the attempt to copy the
	// DPN bag to long-term storage.
	StoreSummary       *apt_models.WorkSummary

	// RecordSummary describes the result of the attempt to record
	// info about the new bag in our local DPN server, and to close
	// out the WorkItem in Pharos.
	RecordSummary      *apt_models.WorkSummary
}

// NewDPNIngestManifest creates a new DPNIngestManifest.
// Param nsqMessage is the nsqMessage being processed.
func NewDPNIngestManifest(nsqMessage *nsq.Message) (*DPNIngestManifest) {
	return &DPNIngestManifest{
		NsqMessage: nsqMessage,
		PackageSummary: apt_models.NewWorkSummary(),
		ValidateSummary: apt_models.NewWorkSummary(),
		StoreSummary: apt_models.NewWorkSummary(),
		RecordSummary: apt_models.NewWorkSummary(),
	}
}
