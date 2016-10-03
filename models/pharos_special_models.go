package models

import (
	"time"
)

// This struct is a special subset of GenericFile, with special JSON
// serialization rules that conform to Rails 4 nested strong paramaters
// naming conventions. When we create GenericFiles in batches, we need
// to send them in this format.
type GenericFileForPharos struct {
	Identifier                   string         `json:"identifier"`
	IntellectualObjectId         int            `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string         `json:"intellectual_object_identifier"`
	FileFormat                   string         `json:"file_format"`
	URI                          string         `json:"uri"`
	Size                         int64          `json:"size"`
	FileCreated                  time.Time      `json:"file_created"`
	FileModified                 time.Time      `json:"file_modified"`
	CreatedAt                    time.Time      `json:"created_at"`
	UpdatedAt                    time.Time      `json:"updated_at"`
	Checksums                    []*Checksum    `json:"checksums_attributes"`
	PremisEvents                 []*PremisEvent `json:"premis_events_attributes"`
}

func NewGenericFileForPharos(gf *GenericFile) (*GenericFileForPharos) {
	return &GenericFileForPharos{
		Identifier:                     gf.Identifier,
		IntellectualObjectId:           gf.IntellectualObjectId,
		IntellectualObjectIdentifier:   gf.IntellectualObjectIdentifier,
		FileFormat:                     gf.FileFormat,
		URI:                            gf.URI,
		Size:                           gf.Size,
		FileCreated:                    gf.FileCreated,
		FileModified:                   gf.FileModified,
		Checksums:                      gf.Checksums,
		PremisEvents:                   gf.PremisEvents,
	}
}
