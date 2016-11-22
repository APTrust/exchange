package models

import (
	"github.com/satori/go.uuid"
	"time"
)

// DPNBag represents a Bag object in the DPN REST service.
// Like all of the DPN REST objects, it contains metadata only.
type DPNBag struct {

	// UUID is the unique identifier for a bag
	UUID string `json:"uuid"`

	// LocalId is the depositor's local identifier for a bag.
	LocalId string `json:"local_id"`

	// Member is the UUID of the member who deposited this bag.
	Member string `json:"member"`

	// Size is the size, in bytes of the bag.
	Size uint64 `json:"size"`

	// FirstVersionUUID is the UUID of the first version
	// of this bag.
	FirstVersionUUID string `json:"first_version_uuid"`

	// Version is the version or revision number of the bag. Starts at 1.
	Version uint32 `json:"version"`

	// IngestNode is the node that first ingested or produced the bag.
	IngestNode string `json:"ingest_node"`

	// AdminNode is the authoritative node for this bag. If various nodes
	// have conflicting registry info for this bag, the admin node wins.
	// The admin node also has some authority in restoring and (if its ever
	// possible) deleting bags.
	AdminNode string `json:"admin_node"`

	// BagType is one of 'D' (Data), 'R' (Rights) or 'I' (Interpretive)
	BagType string `json:"bag_type"`

	// Rights is a list of UUIDs of rights objects for this bag.
	Rights []string `json:"rights"`

	// Interpretive is a list of UUIDs of interpretive objects for this bag.
	Interpretive []string `json:"interpretive"`

	// ReplicatingNodes is a list of one more nodes that has stored
	// copies of this bag. The items in the list are node namespaces,
	// which are strings. E.g. ['aptrust', 'chron', 'tdr']
	ReplicatingNodes []string `json:"replicating_nodes"`

	// MessageDigests are the digests calculated on ingest by various
	// nodes. (Is this calculated on the tag manifest?)
	MessageDigests []*MessageDigest `json:"message_digests"`

	// FixityChecks record the result of post-ingest periodic fixity
	// checks.
	FixityChecks []*FixityCheck `json:"fixity_checks"`

	// CreatedAt is when this record was created.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt time.Time `json:"updated_at"`
}

func NewDPNBag(localId, member, ingestNode string) *DPNBag {
	// AdminNode same is ingest node for newly-ingested
	// bags, per DPN spec.
	_uuid := uuid.NewV4().String()
	return &DPNBag{
		UUID:             _uuid,
		LocalId:          localId,
		Member:           member,
		FirstVersionUUID: _uuid,
		Version:          1,
		IngestNode:       ingestNode,
		AdminNode:        ingestNode,
		BagType:          "D",
		MessageDigests:   make([]*MessageDigest, 0),
		Interpretive:     make([]string, 0),
		Rights:           make([]string, 0),
		ReplicatingNodes: make([]string, 0),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
}
