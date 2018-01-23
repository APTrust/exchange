package models

import (
	"encoding/json"
	"time"
)

// PharosDPNBag represents a lightweight DPN bag record stored in Pharos.
type PharosDPNBag struct {
	Id               int       `json:"id"`
	InstitutionId    int       `json:"institution_id"`
	ObjectIdentifier string    `json:"object_identifier"`
	DPNIdentifier    string    `json:"dpn_identifier"`
	DPNSize          uint64    `json:"dpn_size"`
	Node1            string    `json:"node_1"`
	Node2            string    `json:"node_2"`
	Node3            string    `json:"node_3"`
	DPNCreatedAt     time.Time `json:"dpn_created_at"`
	DPNUpdatedAt     time.Time `json:"dpn_updated_at"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// SerializeForPharos serializes a version of this record that Pharos
// will accept as post/put input. The Pharos post/put serialization
// requires the data to be wrapped in a hash with key 'dpn_bag'.
func (bag *PharosDPNBag) SerializeForPharos() ([]byte, error) {
	data := make(map[string]*PharosDPNBag)
	data["dpn_bag"] = bag
	return json.Marshal(data)
}
