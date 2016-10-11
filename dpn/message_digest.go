package dpn

import (
	"time"
)

// MessageDigest is the digest (usually sha256) of the bag's tagmanifest,
// calculated by a node upon ingest or replication.
type MessageDigest struct {

	// Bag is the UUID of the bag to which this message digest belongs.
	Bag        string     `json:"bag"`

	// Algorithm is the digest algorithm (usually sha256).
	Algorithm  string     `json:"algorithm"`

	// Node is the namespace of the node that calculated this message digest.
	Node       string     `json:"node"`

	// Value is the actual digest value.
	Value      string     `json:"value"`

	// CreatedAt is the DateTime this message digest was created.
	CreatedAt  time.Time  `json:"created_at"`

}
