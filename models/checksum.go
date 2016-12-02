package models

import (
	"encoding/json"
	"fmt"
	"time"
)

/*
Checksum contains information about a checksum that
can be used to validate the integrity of a GenericFile.
DateTime should be in ISO8601 format for local time or UTC
when we serialize an object to JSON for Pharos.

For example:
1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type Checksum struct {
	Id            int       `json:"id,omitempty"` // Do not serialize zero to JSON!
	GenericFileId int       `json:"generic_file_id"`
	Algorithm     string    `json:"algorithm"`
	DateTime      time.Time `json:"datetime"`
	Digest        string    `json:"digest"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// MergeAttributes sets the Id, CreatedAt and UpdatedAt properties of this
// checksum to match those os savedChecksum. We call this after saving a record
// to Pharos, which sets all of those properties. Generally, savedChecksum
// is a temporary record returned from Pharos, while this checksum
// is one we want to keep.
func (checksum *Checksum) MergeAttributes(savedChecksum *Checksum) error {
	if savedChecksum == nil {
		return fmt.Errorf("Param savedChecksum cannot be nil.")
	}
	//fmt.Println("Changing ", checksum.Algorithm, "Id from", checksum.Id, "to", savedChecksum.Id)
	checksum.Id = savedChecksum.Id
	checksum.CreatedAt = savedChecksum.CreatedAt
	checksum.UpdatedAt = savedChecksum.UpdatedAt
	return nil
}

// SerializeForPharos serializes a Checksum into a JSON format that
// the Pharos server will accept for PUT and POST calls.
func (checksum *Checksum) SerializeForPharos() ([]byte, error) {
	pharosObj := NewChecksumForPharos(checksum)
	dataStruct := make(map[string]interface{})
	dataStruct["checksum"] = pharosObj
	return json.Marshal(dataStruct)
}
