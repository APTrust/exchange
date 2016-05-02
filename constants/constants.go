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
type StatusType string

const (
	StatusStarted   StatusType = "Started"
	StatusPending              = "Pending"
	StatusSuccess              = "Success"
	StatusFailed               = "Failed"
	StatusCancelled            = "Cancelled"
)

// Stage enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
type StageType string

const (
	StageRequested StageType = "Requested"
	StageReceive             = "Receive"
	StageFetch               = "Fetch"
	StageUnpack              = "Unpack"
	StageValidate            = "Validate"
	StageStore               = "Store"
	StageRecord              = "Record"
	StageCleanup             = "Cleanup"
	StageResolve             = "Resolve"
)

// Action enumerations match values defined in
// https://github.com/APTrust/fluctus/blob/develop/config/application.rb
type ActionType string

const (
	ActionIngest      ActionType = "Ingest"
	ActionFixityCheck            = "Fixity Check"
	ActionRestore                = "Restore"
	ActionDelete                 = "Delete"
)

type FixityAlgorithmType string

const (
	AlgMd5  FixityAlgorithmType = "md5"
	AlgSha256                   = "sha256"
)

type IdentifierType string

const (
	IdTypeStorageURL  IdentifierType = "url"
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
