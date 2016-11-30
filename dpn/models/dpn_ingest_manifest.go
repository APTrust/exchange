package models

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"time"
)

// DPNIngestManifest contains information about a bag being
// ingested from APTrust to DPN.
type DPNIngestManifest struct {
	// NsqMessage is the NSQ message being processed to fulfill
	// this ingest request.
	NsqMessage *nsq.Message `json:"-"`

	// WorkItem is the WorkItem in Pharos that maintains
	// information about this ingest.
	WorkItem *apt_models.WorkItem

	// WorkItemState is the WorkItemState in Pharos that conntains
	// a JSON representation of this DPNIngestManifest.
	WorkItemState *apt_models.WorkItemState

	// IntellectualObject is the object we are pushing into DPN.
	// We don't serialize this because in cases where the object
	// has tens of thousands of files, this record is huge.
	IntellectualObject *apt_models.IntellectualObject `json:"-"`

	// LocalDir is the directory in which we are assembling the
	// contents of the DPN bag.
	LocalDir string

	// LocalTarFile is the path the tarred DPN bag that we have
	// built, and which we will copy to long-term storage.
	LocalTarFile string

	// StorageURL is the URL of the tarred bag that has been copied
	// into long-term storage.
	StorageURL string

	// DPNBag is the DPNBag record we will create for the bag we're
	// sending into long-term storage. Once the bag is stored, we'll
	// POST this DPNBag record to our local DPN REST server.
	DPNBag *DPNBag

	// PackageSummary describes what happened during the packaging
	// of this DPN bag. The process involves packing all of the
	// IntellectualObject's files into a DPN bag, whose structure
	// differs somewhat from the structure of an APTrust bag.
	PackageSummary *apt_models.WorkSummary

	// ValidateSummary describes the result of running our validator
	// over the DPN bag we just created.
	ValidateSummary *apt_models.WorkSummary

	// StoreSummary describes the result of the attempt to copy the
	// DPN bag to long-term storage.
	StoreSummary *apt_models.WorkSummary

	// RecordSummary describes the result of the attempt to record
	// info about the new bag in our local DPN server, and to close
	// out the WorkItem in Pharos.
	RecordSummary *apt_models.WorkSummary

	// ReplicationTransfers are replication transfer requests we
	// created for this newly ingested DPN bag on our own DPN node.
	// The DPN spec as of late 2016 says that when we create a bag,
	// we need to create two replication transfers with it. That
	// number may change over time. If ReplicationTransfers is
	// empty, the transfers have not yet been created.
	ReplicationTransfers []*ReplicationTransfer

	// DPNIdentifierEvent is the PremisEvent (stored in Pharos)
	// that says we assigned a DPN UUID to this bag. This will
	// be nil until we get to the dpn_ingest_record stage.
	DPNIdentifierEvent *apt_models.PremisEvent

	// DPNIngestEvent is the PremisEvent (stored in Pharos)
	// that says we stored this item in DPN. The StorageURL
	// is in the event's OutcomeDetail. This will
	// be nil until we get to the dpn_ingest_record stage.
	DPNIngestEvent *apt_models.PremisEvent
}

// NewDPNIngestManifest creates a new DPNIngestManifest.
// Param nsqMessage is the nsqMessage being processed.
func NewDPNIngestManifest(nsqMessage *nsq.Message) *DPNIngestManifest {
	return &DPNIngestManifest{
		NsqMessage:           nsqMessage,
		PackageSummary:       apt_models.NewWorkSummary(),
		ValidateSummary:      apt_models.NewWorkSummary(),
		StoreSummary:         apt_models.NewWorkSummary(),
		RecordSummary:        apt_models.NewWorkSummary(),
		ReplicationTransfers: make([]*ReplicationTransfer, 0),
	}
}

// BuildReplicationTransfer returns a new ReplicationTransfer record
// for the DPNBag attached to this manifest Params fromNode and toNode
// become the transfer's FromNode and ToNode. Those should be node
// identifiers, such as "aptrust", "chron", "hathi", "sdr" or "tdr".
// Param link becomes the rsync/ssh link, and should be formatted as
// "<user>@dpn.aptrust.org:outbound/<bag_uuid>.tar".
//
// This returns an error if there is no DPNBag attached to this manifest.
func (manifest *DPNIngestManifest) BuildReplicationTransfer(fromNode, toNode, link string) (*ReplicationTransfer, error) {
	if manifest.DPNBag == nil {
		return nil, fmt.Errorf("Can't build ReplicationTransfer: DPNBag is nil")
	}
	now := time.Now().UTC()
	return &ReplicationTransfer{
		FromNode:        fromNode,
		ToNode:          toNode,
		Bag:             manifest.DPNBag.UUID,
		ReplicationId:   uuid.NewV4().String(),
		FixityAlgorithm: constants.AlgSha256,
		FixityNonce:     nil,
		FixityValue:     nil,
		Protocol:        "rsync",
		Link:            link,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

// BuildDPNIngestEvent returns a PremisEvent saying this bag was ingested
// into DPN, and where it was stored. Returns an error if there is no
// DPNBag attached to this manifest.
func (manifest *DPNIngestManifest) BuildDPNIngestEvent() (*apt_models.PremisEvent, error) {
	if manifest.DPNBag == nil {
		return nil, fmt.Errorf("Can't build DPNIdentifierEvent: DPNBag is nil")
	}
	return &apt_models.PremisEvent{
		Identifier:         uuid.NewV4().String(),
		EventType:          constants.EventIngestion,
		DateTime:           time.Now().UTC(),
		Detail:             "Item ingested into DPN",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      manifest.StorageURL,
		Object:             "APTrust exchange",
		Agent:              "https://github.com/APTrust/exchange",
		OutcomeInformation: fmt.Sprintf("Item stored in DPN at %s", manifest.StorageURL),
	}, nil
}

// BuildDPNIdentifierEvent returns a PremisEvent describing the UUID
// that was assigned to this newly created DPN bag. Returns an error if
// there is no DPNBag attached to this manifest.
func (manifest *DPNIngestManifest) BuildDPNIdentifierEvent() (*apt_models.PremisEvent, error) {
	if manifest.DPNBag == nil {
		return nil, fmt.Errorf("Can't build DPNIngestEvent: DPNBag is nil")
	}
	return &apt_models.PremisEvent{
		Identifier:         uuid.NewV4().String(),
		EventType:          constants.EventIdentifierAssignment,
		DateTime:           time.Now().UTC(),
		Detail:             "Item assigned DPN UUID",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      manifest.DPNBag.UUID,
		Object:             "APTrust exchange using Satori go.uuid",
		Agent:              "https://github.com/satori/go.uuid",
		OutcomeInformation: fmt.Sprintf("Item assigned DPN UUID %s", manifest.DPNBag.UUID),
	}, nil
}
