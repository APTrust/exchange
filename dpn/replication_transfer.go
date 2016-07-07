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
	BagId           string       `json:"uuid"`

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

	// FixityAccept describes whether the FromNode accepts the fixity
	// value calculated by the ToNode. This is a nullable boolean,
	// so it has to be a pointer.
	FixityAccept    *bool        `json:"fixity_accept"`

	// BagValid is a value set by the ToNode to indicate whether
	// the bag it received was valid. This is a nullable boolean,
	// so it has to be a pointer.
	BagValid        *bool        `json:"bag_valid"`

	// Status is the status of the request, which can be any of:
	//
	// "requested"  - The FromNode has requested this transfer.
	//                This means the transfer is new, and no
	//                action has been taken yet.
	// "rejected"   - Set by the ToNode when it will not or cannot
	//                accept this transfer. (Usually due to disk space.)
	// "received"   - Set by the ToNode to indicate it has received the
	//                the bag.
	// "confirmed"  - Set by the FromNode after the bag has been confirmed
	//                valid, the fixity value has been approved, and the bag
	//                has been stored by the ToNode.
	// "stored"     - Set by the ToNode after the bag has been copied to
	//                long-term storage.
	// "cancelled"  - Can be set by either node for any reason. No further
	//                processing should occur on a cancelled request.
	Status          string       `json:"status"`

	// Protocol is the network protocol used to transfer the bag.
	// At launch, the only valid value for this is 'R' for rsync.
	Protocol        string       `json:"protocol"`

	// Link is a URL that the ToNode can use to copy the bag from the
	// FromNode. This value is set by the FromNode.
	Link            string       `json:"link"`

	// CreatedAt is the datetime when this record was created.
	CreatedAt       time.Time    `json:"created_at"`

	// UpdatedAt is the datetime when this record was last updated.
	UpdatedAt       time.Time    `json:"updated_at"`
}
