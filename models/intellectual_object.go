package models

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"strings"
	"time"
)

// IntellectualObject represents a single object (ingested bag) in APTrust.
// An object can include any number of files and events.
//
// Properties are described below, but note that all of the "Ingest-"
// fields populated and used by the Exchange ingest code on ingest only.
// These fields are not stored in Pharos, and will not be populated on
// any IntellectualObject retrieved from Pharos.
//
// Ingest services do save a JSON representation of IntellectualObjects,
// including all of the "Ingest-" fields in the WorkItemState record
// associated with the ingest WorkItem. That JSON record can be useful
// for forensics, debugging, troubleshooting, and data reconstruction.
type IntellectualObject struct {
	// Id is the primary key id of this bag in Pharos.
	// If Id is non-zero, this has been recorded in Pharos.
	Id int `json:"id"`

	// Identifier is the unique bag identifier, which is a
	// string in the format "institution_identifier/bag_name".
	// Example: "virginia.edu/bag1234"
	Identifier string `json:"identifier"`

	// BagName is the name of the bag, without the institution
	// identifier prefix. Example: "bag1234"
	BagName string `json:"bag_name"`

	// Institution is the institution identifier (the domain name)
	// of the institution that owns this bag.
	Institution string `json:"institution"`

	// InstitutionId is the Id (in Pharos) of the institution
	// that owns this bag.
	InstitutionId int `json:"institution_id"`

	// Title is the title of the IntellectualObject. For example,
	// "Architectural Plans for Alderman Library, 1933"
	Title string `json:"title"`

	// Description is a description of the IntellectualObject.
	// This comes from the Internal-Sender-Description field of the
	// bag-info.txt file.
	Description string `json:"description"`

	// Access describes who can see this intellectual object.
	// This is specified in the aptrust-info.txt file. See
	// https://sites.google.com/a/aptrust.org/member-wiki/basic-operations/bagging
	// for a description of access policies.
	Access string `json:"access"`

	// AltIdentifier is an alternate identifier for this bag. It comes from
	// the Internal-Sender-Identifier field in the bag-info.txt file.
	AltIdentifier string `json:"alt_identifier"`

	// DPNUUID is the DPN identifier for this bag, which is a UUID.
	// This field will be empty if the bag has not been pushed to DPN.
	DPNUUID string `json:"dpn_uuid"`

	// ETag is the AWS S3 etag from the depositor's receiving bucket
	// for the bag that became this IntellectualObject.
	ETag string `json:"etag"`

	// GenericFiles is a list of the files that make up this bag.
	GenericFiles []*GenericFile `json:"generic_files"`

	// PremisEvents is a list of PREMIS events associated with this
	// IntellectualObject. That includes events such as ingest and
	// identifier assignment. Note that most PREMIS events are associated
	// with GenericFiles, so see GenericFile.PremisEvents as well.
	PremisEvents []*PremisEvent `json:"events"`

	// CreatedAt is the Pharos timestamp describing when this
	// IntellectualObject was first recorded in our database.
	// This is usually within minutes of the ingest event, after
	// all files have been copied to long-term storage.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt describes when this object was last updated in Pharos.
	// If this timestamp differs from CreatedAt, it usually means the
	// bag (or some part of it) was ingested a second time.
	UpdatedAt time.Time `json:"updated_at"`

	// IngestS3Bucket is the bucket to which the depositor uploaded
	// this bag. We fetch it from there to a local staging area for
	// processing.
	IngestS3Bucket string `json:"ingest_s3_bucket"`

	// IngestS3Key is the file name in the S3 receiving bucket. It
	// should be the bag name plus a ".tar" extension.
	IngestS3Key string `json:"ingest_s3_key"`

	// IngestTarFilePath is the absolute path to the tarred bag in
	// our local staging area. We download the bag (as a tar file) from
	// the receiving bucket to this local file.
	IngestTarFilePath string `json:"ingest_tar_file_path"`

	// IngestUntarredPath is the path to the untarred bag in our local
	// staging area. This may be an empty string if we did not untar the bag.
	// As of APTrust 2.0, we generally validate bags and send their files
	// to long-term storage without ever untarring them.
	IngestUntarredPath string `json:"ingest_untarred_path"`

	// IngestSize is the size of the tarred bag we're trying to ingest.
	IngestSize int64 `json:"ingest_size"`

	// IngestRemoteMd5 is the etag of this bag as reported by the
	// depositor's S3 receiving bucket. We use this to verify the download,
	// if possible. For smaller bags (< 5GB), the etag is an md5 checksum.
	// Large bags that the depositor sent to S3 via multipart
	// upload have an etag that is calculated differently from a normal
	// md5 checksum and includes a dash, followed by the number of parts
	// in the original multipart upload. We cannot use those multipart
	// etags for md5 validation.
	IngestRemoteMd5 string `json:"ingest_remote_md5"`

	// IngestLocalMd5 is the md5 digest of the tarred bag that we calculated
	// locally upon downloading the file.
	IngestLocalMd5 string `json:"ingest_local_md5"`

	// IngestMd5Verified indicates whether or not we were able to verify
	// the md5 digest of the entire bag upon download to our staging area.
	IngestMd5Verified bool `json:"ingest_md5_verified"`

	// IngestMd5Verifiable indicates whether we can verify our local md5
	// digest against the S3 etag for this tarred bag. We cannot verify
	// the checksum of large bags. See the comments on IngestRemoteMd5
	// above.
	IngestMd5Verifiable bool `json:"ingest_md5_verifiable"`

	// IngestManifests is list of manifest files found inside this bag
	// when we downloaded it.
	IngestManifests []string `json:"ingest_manifests"`

	// IngestTagManifests is a list of tag manifests found inside this
	// bag when we downloaded it.
	IngestTagManifests []string `json:"ingest_tag_manifests"`

	// IngestFilesIgnored is a list of files found in the bag that are
	// neither manifests, tag files, or data files. This includes files
	// beginning with a dot (.) or dash (-). We do not save these files
	// to long-term storage.
	IngestFilesIgnored []string `json:"ingest_files_ignored"`

	// IngestTags is a list of tags found in all of the tag files that
	// we parsed when ingesting this bag. We parse only those tag files
	// listed with the ParseAsTagFile option in
	// config/aptrust_bag_validation_config.json. While Pharos itself
	// only keeps and exposes the Title, Description, and Access tags,
	// the JSON ingest data preserved in the WorkItemState record for
	// each ingest includes a record of all tags parsed on ingest for
	// items ingested in APTrust 2.0.
	IngestTags []*Tag `json:"ingest_tags"`

	// IngestMissingFiles is a list of files that appear in the bag's
	// manifest(s) but were not found inside the tarred bag. This list
	// should be empty for valid bags. This field is for reporting
	// bag validation errors.
	IngestMissingFiles []*MissingFile `json:"ingest_missing_files"`

	// IngestTopLevelDirNames is a list of directory names found at
	// the top of the directory hierarchy inside the tarred bag. The
	// APTrust spec says there should be only one directory at the top
	// level of the tar file contents, and that directory should have
	// the same name as the bag, minus the ".tar" extension. This field
	// is used for reporting bag validation errors.
	IngestTopLevelDirNames []string `json:"ingest_top_level_dir_names"`

	// IngestErrorMessage contains information about why ingest failed
	// for this bag. On successful ingest, this field will be empty.
	IngestErrorMessage string `json:"ingest_error_message"`

	// IngestDeletedFromReceivingAt is a timestamp describing when the
	// original tar file was deleted from the receiving bucket. After
	// successful ingest, the workers/apt_recorder process should
	// delete the tar file. If this timestamp is empty, it means the
	// cleanup didn't happen, and we may be accumulating unneeded bags
	// and incurring unnecessary costs in the receiving buckets.
	IngestDeletedFromReceivingAt time.Time `json:"ingest_deleted_from_receiving_at"`

	// genericFileMap is used internally to quickly find GenericFiles by
	// their path within the bag. E.g. "data/photos/image1.jpg".
	genericFileMap map[string]*GenericFile

	// tagMap is used internally to find tags by name.
	tagMap map[string][]*Tag
}

func NewIntellectualObject() *IntellectualObject {
	return &IntellectualObject{
		GenericFiles:       make([]*GenericFile, 0),
		PremisEvents:       make([]*PremisEvent, 0),
		IngestManifests:    make([]string, 0),
		IngestTagManifests: make([]string, 0),
		IngestFilesIgnored: make([]string, 0),
		IngestTags:         make([]*Tag, 0),
	}
}

// MissingFile defines a file that is not in the bag, despite the
// fact that its checksum was found in a manifest. We keep track
// of these during bag validation, so we can report which files
// were not found.
type MissingFile struct {
	Manifest   string
	LineNumber int
	FilePath   string
	Digest     string
}

// NewMissingFile creates a new missing file record.
func NewMissingFile(manifest string, lineNumber int, filePath, digest string) *MissingFile {
	return &MissingFile{
		Manifest:   manifest,
		LineNumber: lineNumber,
		FilePath:   filePath,
		Digest:     digest,
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

func NewTag(sourceFile, label, value string) *Tag {
	return &Tag{
		SourceFile: sourceFile,
		Label:      label,
		Value:      value,
	}
}

// Returns the total number of bytes of all of the generic
// files in this object. The object's bag size will be slightly
// larger than this, because it will include a manifest, tag
// files and tar header.
func (obj *IntellectualObject) TotalFileSize() int64 {
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
func (obj *IntellectualObject) FindGenericFile(filePath string) *GenericFile {
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
func (obj *IntellectualObject) FindTag(tagName string) []*Tag {
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
// Note that GenericFiles marked as IngestNeedsSave = false do
// not need to be saved.
func (obj *IntellectualObject) AllFilesSaved() bool {
	allSaved := true
	for _, gf := range obj.GenericFiles {
		if gf.IngestNeedsSave {
			if gf.IngestStorageURL == "" ||
				gf.IngestReplicationURL == "" ||
				gf.IngestStoredAt.IsZero() ||
				gf.IngestReplicatedAt.IsZero() {
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
// This sets obj.Access to all lower case when it serializes,
// because Pharos requires access values to be normalized that way.
func (obj *IntellectualObject) SerializeForPharos() ([]byte, error) {
	pharosObj := NewIntellectualObjectForPharos(obj)
	dataStruct := make(map[string]interface{})
	dataStruct["intellectual_object"] = pharosObj
	return json.Marshal(dataStruct)
}

// Returns events of the specified type
func (obj *IntellectualObject) FindEventsByType(eventType string) []PremisEvent {
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
func (obj *IntellectualObject) BuildIngestEvents() error {

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
		if gf.IngestNeedsSave {
			err = gf.BuildIngestEvents()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this object was created. There will be some lag time between
// when the object is created and when all of its ingest metadata
// is recorded.
func (obj *IntellectualObject) buildEventCreation() error {
	events := obj.FindEventsByType(constants.EventCreation)
	if len(events) == 0 {
		event := NewEventObjectCreation()
		event.IntellectualObjectId = obj.Id
		event.IntellectualObjectIdentifier = obj.Identifier
		obj.PremisEvents = append(obj.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this object was assigned an identifier, and what that identifier is.
func (obj *IntellectualObject) buildEventIdentifierAssignment() error {
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
func (obj *IntellectualObject) buildEventAccessAssignment() error {
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
func (obj *IntellectualObject) buildEventIngest() error {
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
func (obj *IntellectualObject) BuildIngestChecksums() error {
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
