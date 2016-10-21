package dpn

const (
	STAGE_PRE_COPY  = "Pre Copy"
	STAGE_COPY      = "Copying from ingest node"
	STAGE_PACKAGE   = "Packaging"
	STAGE_RECEIVE   = "Receiving"
	STAGE_VALIDATE  = "Validation"
	STAGE_STORE     = "Storage"
	STAGE_RECORD    = "Record"
	STAGE_COMPLETE  = "Complete"
	STAGE_CANCELLED = "Cancelled"

	DEFAULT_TOKEN_FORMAT_STRING = "token %s"

	BAG_TYPE_DATA = "data"
	BAG_TYPE_RIGHTS = "rights"
	BAG_TYPE_INTERPRETIVE = "interpretive"

	PATH_TYPE_LOCAL = "Local Filesystem"
	PATH_TYPE_S3    = "S3 Bucket"

	// These values are part of the published APTrust spec.
	APTRUST_BAGIT_VERSION = "0.97"
	APTRUST_BAGIT_ENCODING = "UTF-8"
)

type DPNObjectType string

const (
	DPNTypeBag         DPNObjectType = "DPNBag"
	DPNTypeDigest                    = "Digest"
	DPNTypeFixityCheck               = "FixityCheck"
	DPNTypeIngest                    = "Ingest"
	DPNTypeMember                    = "Member"
	DPNTypeNode                      = "Node"
	DPNTypeReplication               = "Replication"
	DPNTypeRestore                   = "Restore"
)

var DPNTypes = []DPNObjectType{
	DPNTypeBag,
	DPNTypeDigest,
	DPNTypeFixityCheck,
	DPNTypeIngest,
	DPNTypeMember,
	DPNTypeNode,
	DPNTypeReplication,
	DPNTypeRestore,
}
