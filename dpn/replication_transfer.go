package dpn

import (
	"time"
)

type ReplicationTransfer struct {

	// FromNode is the node where the bag is coming from.
	// The FromNode initiates the replication request.
	FromNode        string       `json:"from_node"`

	// ToNode is the node the bag is being transfered to
	ToNode          string       `json:"to_node"`

	// Bag is the UUID of the bag to be replicated
	Bag             string       `json:"bag"`

	// ReplicationId is a unique id for this replication request.
	// It's a UUID in string format.
	ReplicationId   string       `json:"replication_id"`

	// FixityAlgorithm is the algorithm used to calculate the fixity digest.
	FixityAlgorithm string       `json:"fixity_algorithm"`

	// FixityNonce is an optional nonce used to calculate the fixity digest.
	FixityNonce     *string      `json:"fixity_nonce"`

	// FixityValue is the fixity value calculated by the ToNode after
	// it receives the bag. This will be null/empty until the replicating
	// node sends the info back to the FromNode.
	FixityValue     *string      `json:"fixity_value"`

	// Protocol is the network protocol used to transfer the bag.
	// At launch, the only valid value for this is 'R' for rsync.
	Protocol        string       `json:"protocol"`

	// Link is a URL that the ToNode can use to copy the bag from the
	// FromNode. This value is set by the FromNode.
	Link            string       `json:"link"`

	// Stored indicates whether the ToNode has stored this bag.
	Stored          bool         `json:"stored"`

	// StoreRequested indicates whether the FromNode wants the ToNode
	// to store the bag. This will be true only after the ToNode has
	// sent the correct FixityValue to the FromNode.
	StoreRequested  bool         `json:"store_requested"`

	// Cancelled indicates whether this replication request was
	// cancelled.
	Cancelled       bool         `json:"cancelled"`

	// CancelReason is free-form text describing why this replication
	// request was cancelled.
	CancelReason	string       `json:"cancel_reason"`

	// CreatedAt is the datetime when this record was created.
	CreatedAt       time.Time    `json:"created_at"`

	// UpdatedAt is the datetime when this record was last updated.
	UpdatedAt       time.Time    `json:"updated_at"`
}
