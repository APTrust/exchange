// Common vars and constants, shared by many parts of the bagman library.
package constants

import (
	"regexp"
)

// The tar files that make up multipart bags include a suffix
// that follows this pattern. For example, after stripping off
// the .tar suffix, you'll have a name like "my_bag.b04.of12"
var MultipartSuffix = regexp.MustCompile("\\.b\\d+\\.of\\d+$")

// APTrustFileNamePattern matches a valid APTrust file name, according to the spec at
// https://sites.google.com/a/aptrust.org/member-wiki/basic-operations/bagging
// This regex says a valid file name can be exactly one alpha-numeric character,
// or 2+ characters, beginning with alpha-numerics or dot or underscore,
// followed by alphanumerics, dots, underscores, dashes and percent signs.
var APTrustFileNamePattern = regexp.MustCompile("^([A-Za-z0-9])$|^([A-Za-z0-9\\._][A-Za-z0-9\\.\\-_%]+)$")

// PosixFileNamePattern matches valid POSIX filenames.
var PosixFileNamePattern = regexp.MustCompile("^[A-Za-z0-9\\._\\-]+$")

// Permissive matches anything that does not contain ASCII bells, form-feeds,
// tabs, newlines, carriage returns or vertical tabs.
var PermissivePattern = regexp.MustCompile("[^\\a\\f\\t\\n\\r\\v]+")

// APTrustSystemUser is the APTrust system user in Pharos.
const APTrustSystemUser = "system@aptrust.org"

// S3LargeFileSize is the largest file size we'll allow Amazon's S3
// chunked uploader to handle. If a file is larger than this, we'll
// chunk it ourselves, so that jackass AWS library doesn't try to read
// the whole thing into memory.
const S3LargeFileSize = 100 * 1024 * 1024 // 100MB

const (
	APTrustNamespace        = "urn:mace:aptrust.org"
	ReceiveBucketPrefix     = "aptrust.receiving."
	ReceiveTestBucketPrefix = "aptrust.receiving.test."
	RestoreBucketPrefix     = "aptrust.restore."
	S3DateFormat            = "2006-01-02T15:04:05.000Z"
	// All S3 urls begin with this.
	S3UriPrefix = "https://s3.amazonaws.com/"
)

// Status enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
const (
	StatusStarted   = "Started"
	StatusPending   = "Pending"
	StatusSuccess   = "Success"
	StatusFailed    = "Failed"
	StatusCancelled = "Cancelled"
)

var StatusTypes []string = []string{
	StatusStarted,
	StatusPending,
	StatusSuccess,
	StatusFailed,
	StatusCancelled,
}

// Stage enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
const (
	StageRequested     = "Requested"
	StageReceive       = "Receive"
	StageFetch         = "Fetch"
	StageUnpack        = "Unpack" // TODO: Delete if we're no longer using this.
	StageValidate      = "Validate"
	StageStore         = "Store"
	StageRecord        = "Record"
	StageCleanup       = "Cleanup"
	StageResolve       = "Resolve"
	StagePackage       = "Package"
	StageRestoring     = "Restoring"
	StageAvailableInS3 = "Available in S3"
)

var StageTypes []string = []string{
	StageRequested,
	StageReceive,
	StageFetch,
	StageUnpack,
	StageValidate,
	StageStore,
	StageRecord,
	StageCleanup,
	StageResolve,
	StagePackage,
	StageRestoring,
	StageAvailableInS3,
}

// Action enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb

const (
	ActionIngest         = "Ingest"
	ActionFixityCheck    = "Fixity Check"
	ActionGlacierRestore = "Glacier Restore"
	ActionRestore        = "Restore"
	ActionDelete         = "Delete"
	ActionDPN            = "DPN"
)

var ActionTypes []string = []string{
	ActionIngest,
	ActionFixityCheck,
	ActionGlacierRestore,
	ActionRestore,
	ActionDelete,
}

// Storage options

const (
	StorageStandard  = "Standard"
	StorageGlacierVA = "Glacier-VA"
	StorageGlacierOH = "Glacier-OH"
	StorageGlacierOR = "Glacier-OR"
)

var StorageOptions []string = []string{
	StorageStandard,
	StorageGlacierVA,
	StorageGlacierOH,
	StorageGlacierOR,
}

// DPN task types
const (
	DPNTaskSync        = "sync"
	DPNTaskIngest      = "ingest"
	DPNTaskReplication = "replication"
	DPNTaskRestore     = "restore"
	DPNTaskFixity      = "fixity"
)

var DPNTaskTypes []string = []string{
	DPNTaskSync,
	DPNTaskIngest,
	DPNTaskReplication,
	DPNTaskRestore,
	DPNTaskFixity,
}

const (
	AlgMd5    = "md5"
	AlgSha256 = "sha256"
)

var ChecksumAlgorithms = []string{AlgMd5, AlgSha256}

const (
	IdTypeStorageURL = "url"
	IdTypeBagAndPath = "bag/filepath"
)

// List of valid APTrust IntellectualObject AccessRights.
var AccessRights []string = []string{
	"consortia",
	"institution",
	"restricted",
}

// AWS Regions (the ones we're using for storage)
const (
	AWSVirginia = "us-east-1"
	AWSOhio     = "us-east-2"
	AWSOregon   = "us-west-2"
)

// GenericFile types. GenericFile.IngestFileType
const (
	PAYLOAD_FILE     = "payload_file"
	PAYLOAD_MANIFEST = "payload_manifest"
	TAG_MANIFEST     = "tag_manifest"
	TAG_FILE         = "tag_file"
)

// PREMIS Event types as defined by the Library of Congress at
// http://id.loc.gov/search/?q=&q=cs%3Ahttp%3A%2F%2Fid.loc.gov%2Fvocabulary%2Fpreservation%2FeventType#
const (
	// The process of assigning access rights.
	// For APTrust, access can be "restricted", "institution" or "consortia".
	// This is not part of the LOC standard, and LOC has no analog for
	// this event. APTrust has been using this event since the repository's
	// inception. In the old system, it was access_assignment.
	EventAccessAssignment = "access assignment"

	// The process whereby a repository actively obtains an object.
	EventCapture = "capture"

	// The process of coding data to save storage space or transmission time.
	EventCompression = "compression"

	// The act of creating a new object.
	EventCreation = "creation"

	// The process of removing an object from the inventory of a repository.
	EventDeaccession = "deaccession"

	// The process of reversing the effects of compression.
	EventDecompression = "decompression"

	//The process of converting encrypted data to plain text.
	EventDecryption = "decryption"

	// The process of removing an object from repository storage.
	EventDeletion = "deletion"

	// The process by which a message digest ("hash") is created.
	// This was fixity_generation in the first iteration of APTrust's
	// software.
	EventDigestCalculation = "message digest calculation"

	// The process of verifying that an object has not been changed in a given period.
	EventFixityCheck = "fixity check"

	// The process of assigning an identifier to an object or file.
	// This one is not in the LOC spec, but APTrust has been using
	// it since the repository's inception, and there is no LOC analog.
	EventIdentifierAssignment = "identifier assignment"

	// The process of adding objects to a preservation repository.
	// Was "ingest" in the first iteration of the repository.
	EventIngestion = "ingestion"

	// A transformation of an object creating a version in a more contemporary format.
	EventMigration = "migration"

	// A transformation of an object creating a version more conducive to preservation.
	EventNormalization = "normalization"

	// The process of creating a copy of an object that is, bit-wise, identical to the original.
	EventReplication = "replication"

	// The process of determining that a decrypted digital signature matches an expected value.
	EventSignatureValidation = "digital signature validation"

	// The process of comparing an object with a standard and noting compliance or exceptions.
	EventValidation = "validation"

	// The process of scanning a file for malicious programs.
	EventVirusCheck = "virus check"
)

var EventTypes []string = []string{
	EventAccessAssignment,
	EventCapture,
	EventCompression,
	EventCreation,
	EventDeaccession,
	EventDecompression,
	EventDecryption,
	EventDeletion,
	EventDigestCalculation,
	EventFixityCheck,
	EventIngestion,
	EventIdentifierAssignment,
	EventMigration,
	EventNormalization,
	EventReplication,
	EventSignatureValidation,
	EventValidation,
	EventVirusCheck,
}

// Event outcomes
const OutcomeSuccess = "Success"
const OutcomeFailure = "Failure"
