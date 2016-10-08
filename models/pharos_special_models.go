package models

import (
	"strings"
	"time"
)

// IntellectualObject in the format that Pharos accepts for
// POST/create.
type IntellectualObjectForPharos struct {
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
	FileFormat                   string         `json:"file_format"`
	URI                          string         `json:"uri"`
	Size                         int64          `json:"size"`
	// TODO: Next two items are not part of Pharos model, but they should be.
	// We need to add these to the Rails schema.
//	FileCreated                  time.Time      `json:"file_created"`
//	FileModified                 time.Time      `json:"file_modified"`
	Checksums                    []*ChecksumForPharos    `json:"checksums_attributes"`
	PremisEvents                 []*PremisEventForPharos `json:"premis_events_attributes"`
}

func NewGenericFileForPharos(gf *GenericFile) (*GenericFileForPharos) {
	checksums := make([]*ChecksumForPharos, len(gf.Checksums))
	for i, cs := range gf.Checksums {
		checksums[i] = NewChecksumForPharos(cs)
	}
	events := make([]*PremisEventForPharos, len(gf.PremisEvents))
	for i, event := range gf.PremisEvents {
		events[i] = NewPremisEventForPharos(event)
	}
	return &GenericFileForPharos{
		Identifier:                     gf.Identifier,
		IntellectualObjectId:           gf.IntellectualObjectId,
		FileFormat:                     gf.FileFormat,
		URI:                            gf.URI,
		Size:                           gf.Size,
		// TODO: See note above. Add these to Rails!
//		FileCreated:                    gf.FileCreated,
//		FileModified:                   gf.FileModified,
		Checksums:                      checksums,
		PremisEvents:                   events,
	}
}

// Same as PremisEvent, but omits CreatedAt and UpdatedAt
type PremisEventForPharos struct {
	Id                 int       `json:"id,omitempty"`
	Identifier         string    `json:"identifier"`
	EventType          string    `json:"event_type"`
	DateTime           time.Time `json:"date_time"`
	Detail             string    `json:"detail"`
	Outcome            string    `json:"outcome"`
	OutcomeDetail      string    `json:"outcome_detail"`
	Object             string    `json:"object"`
	Agent              string    `json:"agent"`
	OutcomeInformation string    `json:"outcome_information"`
	IntellectualObjectId int     `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string `json:"intellectual_object_identifier"`
	GenericFileId int            `json:"generic_file_id"`
	GenericFileIdentifier string `json:"generic_file_identifier"`
}

func NewPremisEventForPharos (event *PremisEvent) (*PremisEventForPharos) {
	return &PremisEventForPharos{
		Id: event.Id,
		Identifier: event.Identifier,
		EventType: event.EventType,
		DateTime: event.DateTime,
		Detail: event.Detail,
		Outcome: event.Outcome,
		OutcomeDetail: event.OutcomeDetail,
		Object: event.Object,
		Agent: event.Agent,
		OutcomeInformation: event.OutcomeInformation,
		IntellectualObjectId: event.IntellectualObjectId,
		IntellectualObjectIdentifier: event.IntellectualObjectIdentifier,
		GenericFileId: event.GenericFileId,
		GenericFileIdentifier: event.GenericFileIdentifier,
	}
}

// Same as Checksum, but without CreatedAt and UpdatedAt
type ChecksumForPharos struct {
	Id            int       `json:"id,omitempty"`  // Do not serialize zero to JSON!
	GenericFileId int       `json:"generic_file_id"`
	Algorithm     string    `json:"algorithm"`
	DateTime      time.Time `json:"datetime"`
	Digest        string    `json:"digest"`
}

func NewChecksumForPharos (cs *Checksum) (*ChecksumForPharos) {
	return &ChecksumForPharos{
		Id: cs.Id,
		GenericFileId: cs.GenericFileId,
		Algorithm: cs.Algorithm,
		DateTime: cs.DateTime,
		Digest: cs.Digest,
	}
}
