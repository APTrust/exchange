package models

import (
	"fmt"
	"encoding/json"
	"strings"
	"time"
)


/*
GenericFile contains information about a file that makes up
part (or all) of an IntellectualObject.
IntellectualObject is the object to which the file belongs.
Format is typically a mime-type, such as "application/xml",
that describes the file format.
URI describes the location of the object (in APTrust?).
Size is the size of the object, in bytes.
Created is the date and time at which the object was created
(in APTrust, or at the institution that owns it?).
Modified is the data and time at which the object was last
modified (in APTrust, or at the institution that owns it?).
Created and Modified should be ISO8601 DateTime strings,
such as:
1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type GenericFile struct {
	// Pharos fields.
	Id                           int            `json:"id"`
	Identifier                   string         `json:"identifier"`
	IntellectualObjectId         int            `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string         `json:"intellectual_object_identifier"`
	FileFormat                   string         `json:"file_format"`
	URI                          string         `json:"uri"`
	Size                         int64          `json:"size"`
	Created                      time.Time      `json:"created"`
	Modified                     time.Time      `json:"modified"`
	Checksums                    []*Checksum    `json:"checksums"`
	PremisEvents                 []*PremisEvent `json:"premis_events"`

	// Exchange fields. These are for internal housekeeping.
	// We don't send this data to Pharos.
	IngestLocalPath              string         `json:"ingest_local_path"`
	IngestMd5                    string         `json:"ingest_md5"`
	IngestMd5Verified            time.Time      `json:"ingest_md5_verified"`
	IngestSha256                 string         `json:"ingest_sha_256"`
	IngestSha256GeneratedAt      time.Time      `json:"ingest_sha_256_generated_at"`
	IngestUUID                   string         `json:"ingest_uuid"`
	IngestUUIDGeneratedAt        time.Time      `json:"ingest_uuid_generated_at"`
	IngestStorageURL             string         `json:"ingest_storage_url"`
	IngestStoredAt               time.Time      `json:"ingest_stored_at"`
	IngestPreviousVersionExists  bool           `json:"ingest_previous_version_exists"`
	IngestNeedsSave              bool           `json:"ingest_needs_save"`
	ErrorMessage                 string         `json:"error_message"`
}

func NewGenericFile() (*GenericFile) {
	return &GenericFile{
		IngestPreviousVersionExists: false,
		IngestNeedsSave: true,
	}
}


// Serializes a version of GenericFile that Fluctus will accept as post/put input.
// Note that we don't serialize the id or any of our internal housekeeping info.
func (gf *GenericFile) SerializeForPharos() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":                     gf.Identifier,
		"intellectual_object_id":         gf.IntellectualObjectId,
		"intellectual_object_identifier": gf.IntellectualObjectIdentifier,
		"file_format":                    gf.FileFormat,
		"uri":                            gf.URI,
		"size":                           gf.Size,
		"created":                        gf.Created,
		"modified":                       gf.Modified,
		"checksum_attributes":            gf.Checksums,
	})
}

// Returns the original path of the file within the original bag.
// This is just the identifier minus the institution id and bag name.
// For example, if the identifier is "uc.edu/cin.675812/data/object.properties",
// this returns "data/object.properties"
func (gf *GenericFile) OriginalPath() (string, error) {
	parts := strings.SplitN(gf.Identifier, "/", 3)
	if len(parts) < 3 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[2], nil
}

// Returns the name of the institution that owns this file.
func (gf *GenericFile) InstitutionIdentifier() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[0], nil
}

// Returns the checksum digest for the given algorithm for this file.
func (gf *GenericFile) GetChecksum(algorithm string) (*Checksum) {
	for _, cs := range gf.Checksums {
		if cs != nil && cs.Algorithm == algorithm {
			return cs
		}
	}
	return nil
}

// Returns events of the specified type
func (gf *GenericFile) FindEventsByType(eventType string) ([]PremisEvent) {
	events := make([]PremisEvent, 0)
	for _, event := range gf.PremisEvents {
		if event != nil && event.EventType == eventType {
			events = append(events, *event)
		}
	}
	return events
}

// Returns the name of this file in the preservation storage bucket
// (that should be a UUID), or an error if the GenericFile does not
// have a valid preservation storage URL.
func (gf *GenericFile) PreservationStorageFileName() (string, error) {
	if strings.Index(gf.URI, "/") < 0 {
		return "", fmt.Errorf("Cannot get preservation storage file name because GenericFile has an invalid URI")
	}
	parts := strings.Split(gf.URI, "/")
	return parts[len(parts) - 1], nil
}
