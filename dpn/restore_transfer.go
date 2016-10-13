package dpn

import (
	"time"
)

type RestoreTransfer struct {

	// RestoreId is the UUID for this restoration request.
	RestoreId       string       `json:"restore_id"`

	// FromNode is the node from which the bag should be restored.
	FromNode        string       `json:"from_node"`

	// ToNode is the node to which the bag should be restored.
	// The ToNode initiates a restoration request.
	ToNode          string       `json:"to_node"`

	// Bag is the unique identifier of the bag to be restored.
	Bag             string       `json:"bag"`

	// Protocol is the network protocol used to transfer the bag.
	// At launch, the only valid value for this is 'R' for rsync.
	Protocol        string       `json:"protocol"`

	// Link is a URL that the ToNode can use to copy the bag from the
	// FromNode. This value is set by the FromNode.
	Link            string       `json:"link"`

	// Accepted indicates whether the FromNode is willing to
	// restore the bag to the ToNode.
	Accepted        bool         `json:"accepted"`

	// Finished indicates whether this restore request has been
	// completed.
	Finished        bool         `json:"finished"`

	// Cancelled indicates whether this restore request was
	// cancelled.
	Cancelled       bool         `json:"cancelled"`

	// CancelReason is free-form text describing why this restore
	// request was cancelled.
	CancelReason	string       `json:"cancel_reason"`


	// CreatedAt is the datetime when this record was created.
	CreatedAt       time.Time    `json:"created_at"`

	// UpdatedAt is the datetime when this record was last updated.
	UpdatedAt       time.Time    `json:"updated_at"`
}
