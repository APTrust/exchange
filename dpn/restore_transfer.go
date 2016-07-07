package dpn

import (
	"time"
)

type RestoreTransfer struct {

	// RestoreId is a unique id for this restoration request.
	RestoreId       string       `json:"restore_id"`

	// FromNode is the node from which the bag should be restored.
	FromNode        string       `json:"from_node"`

	// ToNode is the node to which the bag should be restored.
	// The ToNode initiates a restoration request.
	ToNode          string       `json:"to_node"`

	// Bag is the unique identifier of the bag to be restored.
	BagId           string       `json:"uuid"`

	// Status is the status of the restoration operation. It can
	// have any of the following values:
	//
	// "requested" - Default status used when record first created.
	// "accepted"  - Indicates the FromNode has accepted the request to
	//               restore the bag.
	// "rejected"  - Set by the FromNode if it cannot or will not restore
	//               the bag.
	// "prepared"  - Set by the FromNode when the content has been restored
	//               locally and staged for transfer back to the to_node.
	// "finished"  - Set by the ToNode after it has retrieved the restored
	//               bag from the FromNode.
	// "cancelled" - Set by either node to indicate the restore operation
	//               was cancelled.
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
