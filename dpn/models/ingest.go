package models

import (
	"time"
)

// Ingest describes the completed ingest of a bag, when it
// occurred, and who has copies of it.
type Ingest struct {

	// IngestId is the Id of this ingest record. This is a UUID
	// in string format.
	IngestId string `json:"ingest_id"`

	// Bag is the UUID of the bag to which this record belongs.
	Bag string `json:"bag"`

	// Ingested describes whether or not the ingest process
	// completed successfully. This may be fals in cases where
	// the bag was not successfully replicated to the minimum
	// number of nodes.
	Ingested bool `json:"ingested"`

	// ReplicatingNodes is a list of namespaces of the nodes
	// that have successfully replicated this bag.
	ReplicatingNodes []string `json:"replicating_nodes"`

	// CreatedAt describes when this Ingest record was created
	// in the DPN registry.
	CreatedAt time.Time `json:"created_at"`
}
