package models

import (
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
)

// ReplicationManifest contains information about the processing
// of a DPN ReplicationTransfer request.
type ReplicationManifest struct {
	NsqMessage          *nsq.Message `json:"-"`
	DPNWorkItem         *apt_models.DPNWorkItem
	ReplicationTransfer *ReplicationTransfer
	DPNBag              *DPNBag
	CopySummary         *apt_models.WorkSummary
	ValidateSummary     *apt_models.WorkSummary
	StoreSummary        *apt_models.WorkSummary
	LocalPath           string
	RsyncOutput         string
	Cancelled           bool
}

// NewReplicationManifest creates a new ReplicationManifest.
// Param nsqMessage is the nsqMessage being processed.
func NewReplicationManifest(nsqMessage *nsq.Message) (*ReplicationManifest) {
	return &ReplicationManifest{
		NsqMessage: nsqMessage,
		CopySummary: apt_models.NewWorkSummary(),
		ValidateSummary: apt_models.NewWorkSummary(),
		StoreSummary: apt_models.NewWorkSummary(),
		Cancelled: false,
	}
}
