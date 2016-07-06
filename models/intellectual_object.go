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

	// Exchange fields. These do not go to Pharos.
	IngestS3Bucket         string         `json:"ingest_s3_bucket"`
	IngestS3Key            string         `json:"ingest_s3_key"`
	IngestTarFilePath      string         `json:"ingest_tar_file_path"`
	IngestUntarredPath     string         `json:"ingest_untarred_path"`
	IngestRemoteMd5        string         `json:"ingest_remote_md5"`
	IngestLocalMd5         string         `json:"ingest_local_md5"`
	IngestMd5Verified      bool           `json:"ingest_md5_verified"`
	IngestMd5Verifiable    bool           `json:"ingest_md5_verifiable"`
	IngestManifests        []string       `json:"ingest_manifests"`
	IngestTagManifests     []string       `json:"ingest_tag_manifests"`
	IngestFilesIgnored     []string       `json:"ingest_files_ignored"`
	IngestTags             []*Tag         `json:"ingest_tags"`
	IngestMissingFiles     []*MissingFile `json:"ingest_missing_files"`
	IngestTopLevelDirNames []string     `json:"ingest_top_level_dir_names"`
	IngestErrorMessage     string         `json:"ingest_error_message"`

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
