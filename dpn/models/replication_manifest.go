package models

import (
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
)

// ReplicationManifest contains information about the processing
// of a DPN ReplicationTransfer request.
type ReplicationManifest struct {

	// NsqMessage is the NSQ message being processed to fulfill
	// this replication.
	NsqMessage *nsq.Message `json:"-"`

	// DPNWorkItem is the DPNWorkItem in Pharos that maintains
	// information about this ReplicationTransfer.
	DPNWorkItem *apt_models.DPNWorkItem

	// ReplicationTransfer is the DPN ReplicationTransfer created
	// by a remote node, requesting our local node to replicate
	// a bag.
	ReplicationTransfer *ReplicationTransfer

	// DPNBag is the bag we are replicating.
	DPNBag *DPNBag

	// CopySummary contains information about the process of copying
	// the bag from the remote DPN node to our local staging area
	// for processing.
	CopySummary *apt_models.WorkSummary

	// ValidateSummary contains information about the outcome of
	// the validation process for this bag.
	ValidateSummary *apt_models.WorkSummary

	// StoreSummary contains information about the outcome of the
	// storage operation for this bag. That is, did we manage to
	// copy it to long-term storage or not?
	StoreSummary *apt_models.WorkSummary

	// LocalPath describes where the bag is on our local file system.
	// The bag is a tar file, and should be in the staging area.
	LocalPath string

	// StorageURL describes where the bag is stored in AWS long-term
	// storage. This will be empty until the bag is stored.
	StorageURL string

	// RsyncOutput is the output of the rsync command used to copy
	// the bag from the remote node to our staging area. This is
	// especially useful for debugging, since firewall changes,
	// ssh config problems, and ssh key problems are the main
	// causes of rsync copy failures.
	RsyncOutput string

	// Cancelled indicates whether the replication process was
	// cancelled.
	Cancelled bool
}

// NewReplicationManifest creates a new ReplicationManifest.
// Param nsqMessage is the nsqMessage being processed.
func NewReplicationManifest(nsqMessage *nsq.Message) *ReplicationManifest {
	return &ReplicationManifest{
		NsqMessage:      nsqMessage,
		CopySummary:     apt_models.NewWorkSummary(),
		ValidateSummary: apt_models.NewWorkSummary(),
		StoreSummary:    apt_models.NewWorkSummary(),
		Cancelled:       false,
	}
}
