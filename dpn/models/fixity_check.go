package models

import (
	"time"
)

// FixityCheck represents the result of a post-ingest periodic fixity
// check on a bag's tag manifest. These checks are performed
// every two years by each node.
type FixityCheck struct {

	// FixityCheckId is the unique id of this fixity check record.
	// This is a UUID in string format.
	FixityCheckId  string    `json:"fixity_check_id"`

	// Bag is the UUID of the bag to which this fixity check belongs.
	Bag            string    `json:"bag"`

	// Node is the namespace of the node that performed the fixity check.
	Node           string    `json:"node"`

	// Success indicates whether the calculated fixity matched the
	// expected fixity value (from MessageDigest).
	Success        bool      `json:"success"`

	// FixityAt describes when this fixity check was performed.
	FixityAt       time.Time `json:"fixity_at"`

	// CreatedAt describes when this fixity check record was saved
	// to the DPN registry.
	CreatedAt      time.Time `json:"created_at"`

}
