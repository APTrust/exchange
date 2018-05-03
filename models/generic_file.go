package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
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

FileCreated is the date and time at which the file was created
by the depositor.

FileModified is the data and time at which the object was last
modified (in APTrust, or at the institution that owns it?).

CreatedAt and UpdatedAt are Rails timestamps describing when
this GenericFile records was created and last updated.

FileCreated and FileModified should be ISO8601 DateTime strings,
such as:
1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type GenericFile struct {
	// Pharos fields.

	// The Rails/Database id for this generic file.
	// If the Id is non-zero, it's been recorded in Pharos.
	Id int `json:"id,omitempty"`

	// The human-readable identifier for this file. It consists of
	// the object name, followed by a slash, followed by the path
	// of the file within the bag. E.g. "virginia.edu/bag001/data/file1.pdf"
	Identifier string `json:"identifier,omitempty"`

	// The id of the IntellectualObject to which this file belongs.
	IntellectualObjectId int `json:"intellectual_object_id,omitempty"`

	// The identifier of the intellectual object to which this file belongs.
	IntellectualObjectIdentifier string `json:"intellectual_object_identifier,omitempty"`

	// The file's mime type. E.g. "application/xml"
	FileFormat string `json:"file_format,omitempty"`

	// The location of this file in our primary s3 long-term storage bucket.
	URI string `json:"uri,omitempty"`

	// The size of the file, in bytes.
	Size int64 `json:"size,omitempty"`

	// The date this file was created by the depositor. This date comes from
	// the file record in the tarred bag.
	FileCreated time.Time `json:"file_created,omitempty"`

	// The date this file was last modified by the depository. This date comes
	// from the file record in the tarred bag.
	FileModified time.Time `json:"file_modified,omitempty"`

	// A timestamp indicating when this GenericFile record was created in
	// our repository.
	CreatedAt time.Time `json:"created_at,omitempty"`

	// UpdatedAt indicates when this GenericFile record was last updated in
	// our repository.
	UpdatedAt time.Time `json:"updated_at,omitempty"`

	// Checksums is a list of checksums for this file.
	Checksums []*Checksum `json:"checksums,omitempty"`

	// PremisEvents is a list of PREMIS events for this file.
	PremisEvents []*PremisEvent `json:"premis_events,omitempty"`

	// LastFixityCheck is the date and time we last verified
	// the fixity digest for this file.
	LastFixityCheck time.Time `json:"last_fixity_check,omitempty"`

	// State will be "A" for active files, "D" for deleted files.
	State string `json:"state,omitempty"`

	// Storage option: Standard, Glacier-OH, Glacier-OR, Glacier-VA.
	StorageOption string `json:"storage_option"`

	// ----------------------------------------------------
	// The fields below are for internal housekeeping
	// during the ingest process. We don't send this data
	// to Pharos, and none of the Ingest fields will be
	// populated on GenericFile objects retrieved from
	// Pharos.
	// ----------------------------------------------------

	// IngestFileType can be one of the types defined in constants.
	// PAYLOAD_FILE, PAYLOAD_MANIFEST, TAG_MANIFEST, TAG_FILE
	IngestFileType string `json:"ingest_file_type,omitempty"`

	// IngestLocalPath is the absolute path to this file on local disk.
	// It may be empty if we're working with a tar file.
	IngestLocalPath string `json:"ingest_local_path,omitempty"`

	// IngestManifestMd5 is the md5 checksum of this file, as reported
	// in the bag's manifest-md5.txt file. This may be empty if there
	// was no md5 checksum file, or if this generic file wasn't listed
	// in the md5 manifest.
	IngestManifestMd5 string `json:"ingest_manifest_md5,omitempty"`

	// The md5 checksum we calculated at ingest from the actual file.
	IngestMd5 string `json:"ingest_md5,omitempty"`

	// DateTime we calculated the md5 digest from local file.
	IngestMd5GeneratedAt time.Time `json:"ingest_md5_generated_at,omitempty"`

	// DateTime we verified that our md5 checksum matches what's in the manifest.
	IngestMd5VerifiedAt time.Time `json:"ingest_md5_verified_at,omitempty"`

	// The sha256 checksum for this file, as reported in the payload manifest.
	// This may be empty if the bag had no sha256 manifest, or if this file
	// was not listed in the manifest.
	IngestManifestSha256 string `json:"ingest_manifest_sha256,omitempty"`

	// The sha256 checksum we calculated when we read the actual file.
	IngestSha256 string `json:"ingest_sha_256,omitempty"`

	// Timestamp of when we calculated the sha256 checksum.
	IngestSha256GeneratedAt time.Time `json:"ingest_sha_256_generated_at,omitempty"`

	// Timestamp of when we verified that the sha256 checksum we calculated
	// matches what's in the manifest.
	IngestSha256VerifiedAt time.Time `json:"ingest_sha_256_verified_at,omitempty"`

	// The UUID assigned to this file. This will be its S3 key when we store it.
	IngestUUID string `json:"ingest_uuid,omitempty"`

	// Timestamp of when we generated the UUID for this file. Needed to create
	// the identifier assignment PREMIS event.
	IngestUUIDGeneratedAt time.Time `json:"ingest_uuid_generated_at,omitempty"`

	// Where this file is stored in S3.
	IngestStorageURL string `json:"ingest_storage_url,omitempty"`

	// Timestamp indicating when this file was stored in S3.
	IngestStoredAt time.Time `json:"ingest_stored_at,omitempty"`

	// Where this file is stored in Glacier.
	IngestReplicationURL string `json:"ingest_replication_url,omitempty"`

	// Timestamp indicating when this file was stored in Glacier.
	IngestReplicatedAt time.Time `json:"ingest_replicated_at,omitempty"`

	// If true, a previous version of this same file exists in S3/Glacier.
	IngestPreviousVersionExists bool `json:"ingest_previous_version_exists,omitempty"`

	// If true, this file needs to be saved to S3.
	// We'll set this to false if a copy of the file already
	// exists in long-term storage with the same sha-256 digest.
	IngestNeedsSave bool `json:"ingest_needs_save,omitempty"`

	// Error that occurred during ingest. If empty, there was no error.
	IngestErrorMessage string `json:"ingesterror_message,omitempty"`

	// File User Id (unreliable)
	IngestFileUid int `json:"ingest_file_uid,omitempty"`

	// File Group Id (unreliable)
	IngestFileGid int `json:"ingest_file_gid,omitempty"`

	// File User Name (unreliable)
	IngestFileUname string `json:"ingest_file_uname,omitempty"`

	// File Group Name (unreliable)
	IngestFileGname string `json:"ingest_file_gname,omitempty"`

	// File Mode/Permissions (unreliable)
	IngestFileMode int64 `json:"ingest_file_mode,omitempty"`

	// ----------------------------------------------------
	// The fields below are for internal housekeeping
	// during the restoration, fixity checking, and DPN
	// packaging processes, during which Exchange services
	// download files from long-term storage to run fixity
	// or to rebuild a bag from its component files. The
	// Fetch fields are not saved in Pharos,
	// and GenericFiles retrieved from Pharos will not have
	// Fetch data. Exchange populates those fields
	// as necessary, according to the work it's doing.
	// ----------------------------------------------------

	// FetchLocalPath is the path on the local file system where we
	// saved this file after retrieving it from S3 long-term storage.
	// We only set this when we fetch files to be restored or to be
	// packaged into a DPN bag. When we fetch files for fixity checking,
	// we stream them to /dev/null, because we're only interested in
	// computing a checksum.
	FetchLocalPath string `json:"fetch_local_path,omitempty"`

	// FetchMd5Value is the md5 digest we computed on the file we pulled
	// down from S3. This is supposed to match the file's known md5 fixity
	// value.
	FetchMd5Value string `json:"fetch_md5_value,omitempty"`

	// FetchSha256Value is the sha256 digest we computed on the file we
	// pulled down from S3. This should match the known sha256 digest.
	FetchSha256Value string `json:"fetch_sha256_value,omitempty"`

	// FetchErrorMessage describes any error that occurred during the
	// fetch process, including network errors, object not found, no disk
	// space, fixity mismatches, etc.
	FetchErrorMessage string `json:"fetch_error_message,omitempty"`
}

func NewGenericFile() *GenericFile {
	return &GenericFile{
		Checksums:                   make([]*Checksum, 0),
		PremisEvents:                make([]*PremisEvent, 0),
		IngestPreviousVersionExists: false,
		IngestNeedsSave:             true,
		StorageOption:               constants.StorageStandard,
	}
}

// Serializes a version of GenericFile that Fluctus will accept as post/put input.
// Note that we don't serialize the id or any of our internal housekeeping info.
func (gf *GenericFile) SerializeForPharos() ([]byte, error) {
	genericFileForPharos := NewGenericFileForPharos(gf)
	data := make(map[string]interface{})
	data["generic_file"] = genericFileForPharos
	return json.Marshal(data)
}

// Returns the original path of the file within the original bag.
// This is just the identifier minus the institution id and bag name.
// For example, if the identifier is "uc.edu/cin.675812/data/object.properties",
// this returns "data/object.properties"
func (gf *GenericFile) OriginalPath() string {
	return strings.Replace(gf.Identifier, gf.IntellectualObjectIdentifier+"/", "", 1)
}

// Returns the original path of the file within the original bag,
// including the bag name. This is just the identifier minus the institution id.
// For example, if the identifier is "uc.edu/cin.675812/data/object.properties",
// this returns "cin.675812/data/object.properties"
func (gf *GenericFile) OriginalPathWithBagName() (string, error) {
	instIdentifier, err := gf.InstitutionIdentifier()
	if err != nil {
		return "", err
	}
	return strings.Replace(gf.Identifier, instIdentifier+"/", "", 1), nil
}

// Returns the name of the institution that owns this file.
func (gf *GenericFile) InstitutionIdentifier() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("GenericFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[0], nil
}

// Returns the LAST checksum digest for the given algorithm for this file.
func (gf *GenericFile) GetChecksumByAlgorithm(algorithm string) *Checksum {
	var checksum *Checksum
	latest := time.Time{}
	for _, cs := range gf.Checksums {
		if cs != nil && cs.Algorithm == algorithm && cs.DateTime.After(latest) {
			checksum = cs
			latest = cs.DateTime
		}
	}
	return checksum
}

// Returns the LAST checksum with the given digest for this file.
func (gf *GenericFile) GetChecksumByDigest(digest string) *Checksum {
	for _, cs := range gf.Checksums {
		if cs != nil && cs.Digest == digest {
			return cs
		}
	}
	return nil
}

// Returns events of the specified type
func (gf *GenericFile) FindEventsByType(eventType string) []*PremisEvent {
	events := make([]*PremisEvent, 0)
	for _, event := range gf.PremisEvents {
		if event != nil && event.EventType == eventType {
			events = append(events, event)
		}
	}
	return events
}

// Returns the event with the matching identifier (UUID)
func (gf *GenericFile) FindEventByIdentifier(identifier string) *PremisEvent {
	var matchingEvent *PremisEvent
	for _, event := range gf.PremisEvents {
		if event.Identifier == identifier {
			matchingEvent = event
			break
		}
	}
	return matchingEvent
}

// Merge attributes from a recently-saved GenericFile into this one.
// When we save a GenericFile in Pharos, it assigns attributes Id,
// CreatedAt, and UpdatedAt. This function will save those attributes
// into the current object, and will also call MergeAttributes on
// this file's child objects (PremisEvents and Checksums), if the
// savedGenericFile has PremisEvents and Checksums. This also propagates
// the new Id attribute to the GenericFile's children. Generally,
// savedGenericFile is a disposable data structure that we throw away
// after merging its attributes into this object.
func (gf *GenericFile) MergeAttributes(savedFile *GenericFile) []error {
	errors := make([]error, 0)
	gf.Id = savedFile.Id
	gf.CreatedAt = savedFile.CreatedAt
	gf.UpdatedAt = savedFile.UpdatedAt
	gf.PropagateIdsToChildren()
	for _, savedEvent := range savedFile.PremisEvents {
		event := gf.FindEventByIdentifier(savedEvent.Identifier)
		if event == nil {
			err := fmt.Errorf("After save, could not find event '%s' "+
				"in GenericFile %s.", savedEvent.Identifier, gf.Identifier)
			errors = append(errors, err)
			continue
		}
		err := event.MergeAttributes(savedEvent)
		if err != nil {
			errors = append(errors, err)
		}
	}
	for _, savedChecksum := range savedFile.Checksums {
		checksum := gf.GetChecksumByDigest(savedChecksum.Digest)
		if checksum == nil {
			err := fmt.Errorf("After save, could not find %s "+
				"checksum in GenericFile %s.", savedChecksum.Algorithm, gf.Identifier)
			errors = append(errors, err)
			continue
		}
		err := checksum.MergeAttributes(savedChecksum)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

// Returns the name of this file in the preservation storage bucket
// (that should be a UUID), or an error if the GenericFile does not
// have a valid preservation storage URL.
func (gf *GenericFile) PreservationStorageFileName() (string, error) {
	if strings.Index(gf.URI, "/") < 0 {
		return "", fmt.Errorf("Cannot get preservation storage file name because GenericFile has an invalid URI")
	}
	parts := strings.Split(gf.URI, "/")
	return parts[len(parts)-1], nil
}

// BuildIngestEvents creates all of the ingest events for
// this GenericFile. See the notes for IntellectualObject.BuildIngestEvents,
// as they all apply here. This call is idempotent, so
// calling it multiple times will not mess up our data.
func (gf *GenericFile) BuildIngestEvents() error {

	err := gf.buildIngestFixityCheck()
	if err != nil {
		return err
	}

	err = gf.buildDigestCalculationEvent()
	if err != nil {
		return err
	}

	// TODO: This should not be built if file already exists.
	err = gf.buildFileIdentifierAssignmentEvent()
	if err != nil {
		return err
	}

	// TODO: Should we assign a new UUID and create this
	// event if we're updating an existing file?
	err = gf.buildS3URLAssignmentEvent()
	if err != nil {
		return err
	}

	// There is no replication for Glacier-only storage.
	if gf.StorageOption == constants.StorageStandard {
		err = gf.buildReplicationEvent()
		if err != nil {
			return err
		}
	}

	err = gf.buildFileIngestEvent()
	if err != nil {
		return err
	}

	return nil
}

// Builds an event (if it does not already exist) saying
// that we validated the md5 checksum in the manifest-md5.txt
// file against the checksum of the file itself.
func (gf *GenericFile) buildIngestFixityCheck() error {
	events := gf.FindEventsByType(constants.EventFixityCheck)
	if len(events) == 0 {
		timestamp := gf.IngestSha256VerifiedAt
		if timestamp.IsZero() {
			timestamp = gf.IngestMd5VerifiedAt
		}
		fixityMatched := !timestamp.IsZero()
		event, err := NewEventGenericFileFixityCheck(timestamp,
			constants.AlgMd5, gf.IngestMd5, fixityMatched)
		if err != nil {
			return fmt.Errorf("Error building fixity check event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
		gf.LastFixityCheck = timestamp
	}
	return nil
}

// Builds an event (if it doesn't already exist) describing
// when we calculated the sha256 checksum for this file, and
// what the digest was.
func (gf *GenericFile) buildDigestCalculationEvent() error {
	events := gf.FindEventsByType(constants.EventDigestCalculation)
	if len(events) == 0 {
		event, err := NewEventGenericFileDigestCalculation(
			gf.IngestSha256GeneratedAt, constants.AlgSha256, gf.IngestSha256)
		if err != nil {
			return fmt.Errorf("Error building replication event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
	}
	return nil
}

// Builds the identifier assignment event saying we assigned a
// GenericFile identifier (school.edu/bag_name), only if that
// event does not already exist.
func (gf *GenericFile) buildFileIdentifierAssignmentEvent() error {
	events := gf.FindEventsByType(constants.EventIdentifierAssignment)
	hasIdentifierAssignment := false
	for _, existingEvent := range events {
		// If the identifier is not a URL, it's the file identifier
		if !existingEvent.IsUrlAssignment() {
			hasIdentifierAssignment = true
		}
	}
	// Identifier Assignment (file identifier: school.edu/bag_name)
	// We have to generate for all new generic files, but not when
	// we are overwriting a previously existing generic file. In
	// that case, the identifier was generated when the file was
	// initially ingested.
	if !hasIdentifierAssignment && !gf.IngestPreviousVersionExists {
		event, err := NewEventGenericFileIdentifierAssignment(
			gf.IngestUUIDGeneratedAt, constants.IdTypeBagAndPath,
			gf.Identifier)
		if err != nil {
			return fmt.Errorf("Error building file identifier assignment event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) saying when
// we assigned the S3 storage URL to this file.
func (gf *GenericFile) buildS3URLAssignmentEvent() error {
	events := gf.FindEventsByType(constants.EventIdentifierAssignment)
	hasS3URLAssignment := false
	for _, existing_event := range events {
		if strings.HasPrefix(existing_event.OutcomeDetail, "http://") ||
			strings.HasPrefix(existing_event.OutcomeDetail, "https://") {
			hasS3URLAssignment = true
		}
	}
	// The URL of this item in S3 (primary long-term storage)
	// No need to build URL identifier assignment if we're
	// overwriting an existing URL with a new version of a file.
	if !hasS3URLAssignment && gf.IngestNeedsSave {
		event, err := NewEventGenericFileIdentifierAssignment(
			gf.IngestStoredAt,
			constants.IdTypeStorageURL,
			gf.IngestStorageURL)
		if err != nil {
			return fmt.Errorf("Error building S3 URL identifier assignment event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// we replicated this file to Glacier.
func (gf *GenericFile) buildReplicationEvent() error {
	// The URL of this item in Glacier (secondard long-term storage,
	// AKA replication)
	events := gf.FindEventsByType(constants.EventReplication)
	if len(events) == 0 {
		event, err := NewEventGenericFileReplication(
			gf.IngestReplicatedAt, gf.IngestReplicationURL)
		if err != nil {
			return fmt.Errorf("Error building replication event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
	}
	return nil
}

// Builds the event (if it doesn't already exist) describing when
// this file was ingested.
func (gf *GenericFile) buildFileIngestEvent() error {
	// Item has completed all steps of ingest.
	events := gf.FindEventsByType(constants.EventIngestion)
	if len(events) == 0 {
		event, err := NewEventGenericFileIngest(gf.IngestStoredAt, gf.IngestMd5, gf.IngestUUID)
		if err != nil {
			return fmt.Errorf("Error building ingest event for %s: %v",
				gf.Identifier, err)
		}
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		gf.PremisEvents = append(gf.PremisEvents, event)
	}
	return nil
}

// BuildIngestChecksums creates all of the ingest checksums for
// this GenericFile. See the notes for IntellectualObject.BuildIngestEvents,
// as they all apply here. This call is idempotent, so
// calling it multiple times will not mess up our data.
func (gf *GenericFile) BuildIngestChecksums() error {
	err := gf.buildIngestMd5()
	if err != nil {
		return err
	}
	err = gf.buildIngestSha256()
	if err != nil {
		return err
	}
	return nil
}

// Creates the initial md5 Checksum record for this file, if
// it does not already exist.
func (gf *GenericFile) buildIngestMd5() error {
	md5 := gf.GetChecksumByAlgorithm(constants.AlgMd5)
	if md5 == nil {
		if len(gf.IngestMd5) != 32 {
			return fmt.Errorf("Cannot create md5 Checksum object: "+
				"IngestMd5 '%s' is missing or invalid.", gf.IngestMd5)
		}
		if gf.IngestMd5GeneratedAt.IsZero() {
			return fmt.Errorf("Cannot create md5 Checksum object: " +
				"IngestMd5GeneratedAt is missing.")
		}
		md5 = &Checksum{
			Algorithm:     constants.AlgMd5,
			DateTime:      gf.IngestMd5GeneratedAt,
			Digest:        gf.IngestMd5,
			GenericFileId: gf.Id,
		}
		gf.Checksums = append(gf.Checksums, md5)
	}
	return nil
}

// Creates the initial sha256 Checksum record for this file, if
// it does not already exist.
func (gf *GenericFile) buildIngestSha256() error {
	sha256 := gf.GetChecksumByAlgorithm(constants.AlgSha256)
	if sha256 == nil {
		if len(gf.IngestSha256) != 64 {
			return fmt.Errorf("Cannot create sha256 Checksum object: "+
				"IngestSha256 '%s' is missing or invalid.", gf.IngestSha256)
		}
		if gf.IngestSha256GeneratedAt.IsZero() {
			return fmt.Errorf("Cannot create sha256 Checksum object: " +
				"IngestSha256GeneratedAt is missing.")
		}
		sha256 = &Checksum{
			Algorithm:     constants.AlgSha256,
			DateTime:      gf.IngestSha256GeneratedAt,
			Digest:        gf.IngestSha256,
			GenericFileId: gf.Id,
		}
		gf.Checksums = append(gf.Checksums, sha256)
	}
	return nil
}

// Copy this GenericFile's Id and Identifier to the GenericFileId
// and GenericFileIdentifier properties of all child objects,
// including Checksums and Premis Events. This call exists because
// GenericFiles don't have Ids until after they're saved in
// Pharos.
func (gf *GenericFile) PropagateIdsToChildren() {
	for _, event := range gf.PremisEvents {
		event.GenericFileId = gf.Id
		event.GenericFileIdentifier = gf.Identifier
		event.IntellectualObjectId = gf.IntellectualObjectId
		event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
	}
	for _, checksum := range gf.Checksums {
		checksum.GenericFileId = gf.Id
	}
}
