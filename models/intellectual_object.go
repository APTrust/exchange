package models

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"strings"
	"time"
)

/*
IntellectualObject is Fluctus' version of an IntellectualObject.
It belongs to an Institution and consists of one or more
GenericFiles and a number of events.
Institution is the owner of the intellectual object.
Title is the title.
Description is a free-text description of the object.
Identifier is the object's unique identifier. (Whose assigned
this id? APTrust or the owner?)
Access indicate who can access the object. Valid values are
consortial, institution and restricted.
*/
type IntellectualObject struct {
	// If Id is non-zero, this has been recorded in Pharos.
	Id                     int            `json:"id"`
	Identifier             string         `json:"identifier"`
	BagName                string         `json:"bag_name"`
	Institution            string         `json:"institution"`
	InstitutionId          int            `json:"institution_id"`
	Title                  string         `json:"title"`
	Description            string         `json:"description"`
	Access                 string         `json:"access"`
	AltIdentifier          string         `json:"alt_identifier"`
	GenericFiles           []*GenericFile `json:"generic_files"`
	PremisEvents           []*PremisEvent `json:"events"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`

	// The following are fields populated and used by the Exchange
	// ingest code on ingest only. These fields are not stored in
	// Pharos, and will not be populated on any IntellectualObject
	// retrieved from Pharos.
	IngestS3Bucket         string         `json:"ingest_s3_bucket"`
	IngestS3Key            string         `json:"ingest_s3_key"`
	IngestTarFilePath      string         `json:"ingest_tar_file_path"`
	IngestUntarredPath     string         `json:"ingest_untarred_path"`
	IngestSize             int64          `json:"ingest_size"`
	IngestRemoteMd5        string         `json:"ingest_remote_md5"`
	IngestLocalMd5         string         `json:"ingest_local_md5"`
	IngestMd5Verified      bool           `json:"ingest_md5_verified"`
	IngestMd5Verifiable    bool           `json:"ingest_md5_verifiable"`
	IngestManifests        []string       `json:"ingest_manifests"`
	IngestTagManifests     []string       `json:"ingest_tag_manifests"`
	IngestFilesIgnored     []string       `json:"ingest_files_ignored"`
	IngestTags             []*Tag         `json:"ingest_tags"`
	IngestMissingFiles     []*MissingFile `json:"ingest_missing_files"`
	IngestTopLevelDirNames []string       `json:"ingest_top_level_dir_names"`
	IngestErrorMessage     string         `json:"ingest_error_message"`
	IngestDeletedFromReceivingAt time.Time `json:"ingest_deleted_from_receiving_at"`

	genericFileMap         map[string]*GenericFile
	tagMap                 map[string][]*Tag
}

func NewIntellectualObject() (*IntellectualObject) {
	return &IntellectualObject{
		GenericFiles: make([]*GenericFile, 0),
		PremisEvents: make([]*PremisEvent, 0),
		IngestManifests: make([]string, 0),
		IngestTagManifests: make([]string, 0),
		IngestFilesIgnored: make([]string, 0),
		IngestTags: make([]*Tag, 0),
	}
}

// MissingFile defines a file that is not in the bag, despite the
// fact that its checksum was found in a manifest.
type MissingFile struct {
	Manifest    string
	LineNumber  int
	FilePath    string
	Digest      string
}

func NewMissingFile(manifest string, lineNumber int, filePath, digest string) (*MissingFile) {
	return &MissingFile{
		Manifest: manifest,
		LineNumber: lineNumber,
		FilePath: filePath,
		Digest: digest,
	}
}

// This Tag struct is essentially the same as the bagins
// TagField struct, but its properties are public and can
// be easily serialized to / deserialized from JSON.
type Tag struct {
	SourceFile string
	Label      string
	Value      string
}

func NewTag(sourceFile, label, value string) (*Tag) {
	return &Tag{
		SourceFile: sourceFile,
		Label: label,
		Value: value,
	}
}

// Returns the total number of bytes of all of the generic
// files in this object. The object's bag size will be slightly
// larger than this, because it will include a manifest, tag
// files and tar header.
func (obj *IntellectualObject) TotalFileSize() (int64) {
	total := int64(0)
	for _, genericFile := range obj.GenericFiles {
		total += genericFile.Size
	}
	return total
}

// AccessValid returns true or false to indicate whether the
// structure's Access property contains a valid value.
func (obj *IntellectualObject) AccessValid() bool {
	lcAccess := strings.ToLower(obj.Access)
	for _, value := range constants.AccessRights {
		if value == lcAccess {
			return true
		}
	}
	return false
}

// Returns the GenericFile record for the specified path, or nil.
// Param filePath should be the relative path of the file within
// the bag. E.g. "data/images/myphoto.jpg"
func (obj *IntellectualObject) FindGenericFile(filePath string) (*GenericFile) {
	if obj.genericFileMap == nil || len(obj.genericFileMap) != len(obj.GenericFiles) {
		obj.genericFileMap = make(map[string]*GenericFile, len(obj.GenericFiles))
		for i := range obj.GenericFiles {
			gf := obj.GenericFiles[i]
			obj.genericFileMap[gf.OriginalPath()] = gf
		}
	}
	return obj.genericFileMap[filePath]
}

// Returns the tag with the specified name, or nil. The bag spec at
// https://tools.ietf.org/html/draft-kunze-bagit-13#section-2.2.2
// says tags may be repeated, and their order must be preserved,
// so this returns a slice of tags if the tag is found. In most
// cases, the slice will contain one element.
func (obj *IntellectualObject) FindTag(tagName string) ([]*Tag) {
	if obj.tagMap == nil {
		obj.tagMap = make(map[string][]*Tag)
		for i := range obj.IngestTags {
			tag := obj.IngestTags[i]
			if obj.tagMap[tag.Label] == nil {
				obj.tagMap[tag.Label] = make([]*Tag, 0)
			}
			obj.tagMap[tag.Label] = append(obj.tagMap[tag.Label], tag)
		}
	}
	return obj.tagMap[tagName]
}

// Returns true if all the GenericFiles that needed to be saved
// were successfully saved to both primary and secondary storage.
// Note that GenericFiles maked as IngestNeedsSave = false do
// not need to be saved.
func (obj *IntellectualObject) AllFilesSaved() (bool) {
	allSaved := true
	for _, gf := range obj.GenericFiles {
		if gf.IngestNeedsSave {
			if (gf.IngestStorageURL == "" ||
				gf.IngestReplicationURL == "" ||
				gf.IngestStoredAt.IsZero() ||
				gf.IngestReplicatedAt.IsZero()) {
				allSaved = false
				break
			}
		}
	}
	return allSaved
}

// Serialize the subset of IntellectualObject data that Pharos
// will accept. This is for post/put, where essential info, such
// as institution id and/or object id will be in the URL.
func (obj *IntellectualObject) SerializeForPharos() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":     obj.Identifier,
		"bag_name":       obj.BagName,
		"institution":    obj.Institution,
		"title":          obj.Title,
		"description":    obj.Description,
		"alt_identifier": obj.AltIdentifier,
		"access":         obj.Access,
	})
}

// Returns events of the specified type
func (obj *IntellectualObject) FindEventsByType(eventType string) ([]PremisEvent) {
	events := make([]PremisEvent, 0)
	for _, event := range obj.PremisEvents {
		if event != nil && event.EventType == eventType {
			events = append(events, *event)
		}
	}
	return events
}


// BuildIngestEvents creates all of the PremisEvents required
// for ingest for this IntellectualObject and all of its
// GenericFiles. This call works only when the Ingest data fields
// on the IntellectualObject are populated, which means it will
// not work on the barebones IntellectualObject we get back from
// Pharos. It will work on the IntellectualObject we build during
// the ingest process, the one that apt_fetch builds and passes
// along to apt_store and apt_record. That fully-fleshed object
// is preserved in JSON format in WorkItemState.State.
//
// We want to build all of the ingest PremisEvents before saving
// them to avoid a problem that showed up in the old system. In
// that system, we created PremisEvents when we were ready to
// save them. Ingest often took 2 or 3 attempts in the old system
// due to problems with Fluctus/Fedora. That resulted in 2 or 3
// ingest events for each object and file. Generating the events
// beforehand, with uuids, allows us to check with Pharos first
// to see if the event with the specific uuid is already in the
// system. We can add it if it's not, and we won't duplicate it
// if it is. This takes care of PT #113562325.
//
// This call is idempotent, so calling it multiple times will
// not mess up our data.
func (obj *IntellectualObject) BuildIngestEvents() (error) {

	err := obj.buildEventCreation()
	if err != nil {
		return err
	}

	err = obj.buildEventIdentifierAssignment()
	if err != nil {
		return err
	}

	err = obj.buildEventAccessAssignment()
	if err != nil {
		return err
	}

	err = obj.buildEventIngest()
	if err != nil {
		return err
	}

	for _, gf := range obj.GenericFiles {
		err = gf.BuildIngestEvents()
		if err != nil {
			return err
		}
	}

	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this object was created. There will be some lag time between
// when the object is created and when all of its ingest metadata
// is recorded.
func (obj *IntellectualObject) buildEventCreation() (error) {
	events := obj.FindEventsByType(constants.EventCreation)
	if len(events) == 0 {
		event, err := NewEventObjectCreation()
		if err != nil {
			return err
		}
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
		obj.PremisEvents = append(obj.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this object was assigned an identifier, and what that identifier is.
func (obj *IntellectualObject) buildEventIdentifierAssignment() (error) {
	events := obj.FindEventsByType(constants.EventIdentifierAssignment)
	if len(events) == 0 {
		event, err := NewEventObjectIdentifierAssignment(obj.Identifier)
		if err != nil {
			return err
		}
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
		obj.PremisEvents = append(obj.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// access permissions were set on this object.
func (obj *IntellectualObject) buildEventAccessAssignment() (error) {
	events := obj.FindEventsByType(constants.EventAccessAssignment)
	if len(events) == 0 {
		event, err := NewEventObjectRights(obj.Access)
		if err != nil {
			return err
		}
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
		obj.PremisEvents = append(obj.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this object was fully ingested.
func (obj *IntellectualObject) buildEventIngest() (error) {
	events := obj.FindEventsByType(constants.EventIngestion)
	if len(events) == 0 {
		event, err := NewEventObjectIngest(len(obj.GenericFiles))
		if err != nil {
			return err
		}
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
		obj.PremisEvents = append(obj.PremisEvents, event)
	}
	return nil
}

// BuildIngestChecksums creates all of the ingest checksums for
// this object's GenericFiles. See the notes for BuildIngestEvents
// above, as they all apply here. This call is idempotent, so
// calling it multiple times will not mess up our data.
func (obj *IntellectualObject) BuildIngestChecksums() (error) {
	for _, gf := range obj.GenericFiles {
		err := gf.BuildIngestChecksums()
		if err != nil {
			return err
		}
	}
	return nil
}

// Copy this IntellectualObject's Id and Identifier to the
// IntellectualObjectId and IntellectualObjectIdentifier
// properties of all child objects. Also propagates GenericFile
// Ids and Identifiers to sub-objects, if they are avialable.
// This call exists because objects don't have Ids until after
// they're saved in Pharos
func (obj *IntellectualObject) PropagateIdsToChildren() {
	for _, event := range obj.PremisEvents {
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
	}
	for _, gf := range obj.GenericFiles {
		gf.IntellectualObjectId = obj.Id
		gf.IntellectualObjectIdentifier = obj.Identifier
		gf.PropagateIdsToChildren()
	}
}
