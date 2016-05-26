// Common vars and constants, shared by many parts of the bagman library.
package constants

import (
	"regexp"
)


// The tar files that make up multipart bags include a suffix
// that follows this pattern. For example, after stripping off
// the .tar suffix, you'll have a name like "my_bag.b04.of12"
var MultipartSuffix = regexp.MustCompile("\\.b\\d+\\.of\\d+$")

const (
	APTrustNamespace        = "urn:mace:aptrust.org"
	ReceiveBucketPrefix     = "aptrust.receiving."
	ReceiveTestBucketPrefix = "aptrust.receiving.test."
	RestoreBucketPrefix     = "aptrust.restore."
	S3DateFormat            = "2006-01-02T15:04:05.000Z"
	// All S3 urls begin with this.
	S3UriPrefix             = "https://s3.amazonaws.com/"
)


// Status enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
const (
	StatusStarted              = "Started"
	StatusPending              = "Pending"
	StatusSuccess              = "Success"
	StatusFailed               = "Failed"
	StatusCancelled            = "Cancelled"
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
	StageRequested           = "Requested"
	StageReceive             = "Receive"
	StageFetch               = "Fetch"
	StageUnpack              = "Unpack"
	StageValidate            = "Validate"
	StageStore               = "Store"
	StageRecord              = "Record"
	StageCleanup             = "Cleanup"
	StageResolve             = "Resolve"
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
}

// Action enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb

const (
	ActionIngest                 = "Ingest"
	ActionFixityCheck            = "Fixity Check"
	ActionRestore                = "Restore"
	ActionDelete                 = "Delete"
)

var ActionTypes []string = []string{
	ActionIngest,
	ActionFixityCheck,
	ActionRestore,
	ActionDelete,
}


const (
	AlgMd5                      = "md5"
	AlgSha256                   = "sha256"
)

var ChecksumAlgorithms = []string{ AlgMd5, AlgSha256 }

const (
	IdTypeStorageURL                 = "url"
	IdTypeBagAndPath                 = "uuid"
)

// List of valid APTrust IntellectualObject AccessRights.
var AccessRights []string = []string{
	"consortia",
	"institution",
	"restricted",
}

// List of valid Premis Event types.
var EventTypes []string = []string{
	"ingest",
	"validation",
	"fixity_generation",
	"fixity_check",
	"identifier_assignment",
	"quarentine",
	"delete_action",
	"replication",
}

const (
	AWSVirginia = "us-east-1"
	AWSOregon = "us-west-2"
)

// GenericFile types. GenericFile.IngestFileType
const (
	PAYLOAD_FILE     = "payload_file"
	PAYLOAD_MANIFEST = "payload_manifest"
	TAG_MANIFEST     = "tag_manifest"
	TAG_FILE         = "tag_file"
)
