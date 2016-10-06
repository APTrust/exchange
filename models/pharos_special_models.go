package models

import (
	"strings"
	"time"
)

// IntellectualObject in the format that Pharos accepts for
// POST/create.
type IntellectualObjectForPharos struct {
	Id             int    `json:"id"`
	Identifier     string `json:"identifier"`
	BagName        string `json:"bag_name"`
	InstitutionId  int    `json:"institution_id"`
	Title          string `json:"title"`
	Description    string `json:"description"`
	AltIdentifier  string `json:"alt_identifier"`
	Access         string `json:"access"`
}

func NewIntellectualObjectForPharos(obj *IntellectualObject) (*IntellectualObjectForPharos) {
	return &IntellectualObjectForPharos{
		Id: obj.Id,
		Identifier: obj.Identifier,
		BagName: obj.BagName,
		InstitutionId: obj.InstitutionId,
		Title: obj.Title,
		Description: obj.Description,
		AltIdentifier: obj.AltIdentifier,
		Access: strings.ToLower(obj.Access), // Note that Pharos wants lowercase
	}
}

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
