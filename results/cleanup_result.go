package results

import (
	"github.com/APTrust/exchange/models"
	"time"
)

// CleanupResult describes one or more files to be
// deleted from S3, and the result of the attempts
// to delete those files. The combination of BagName +
// ETag + BagDate maps to a unique entry in Fluctus'
// ProcessedItems table.
type CleanupResult struct {
	// The S3 key of the original bag file. This will
	// be in one of the receiving buckets. This is not
	// necessarily the file we'll be deleting, but all
	// files to be deleted are related to this bag.
	BagName            string
	// The ETag of the original uploaded bag (minus the
	// quotes). This is the bag's md5 sum for bags under
	// about 2GB.
	ETag               string
	// The modified date of the original bag.
	BagDate            time.Time
	// The identifier of the intellectual object to which
	// the Files belong. This may be an empty string in
	// cases where we're cleaning up files from a bag that
	// failed ingest. If it's not null, the bag was successfully
	// ingested, and the identifier will look something like
	// virginia.edu/bag_name
	ObjectIdentifier   string
	// Files contains a list of files/keys to be deleted
	// from S3.
	Files              []*models.S3File
}

// Returns true if all files were successfully deleted.
func (result *CleanupResult) Succeeded() bool {
	for _, file := range result.Files {
		if file.DeleteSkippedPerConfig == true {
			continue
		}
		if file.DeleteAttempted() == false || file.DeletedAt.IsZero() == true {
			return false
		}
	}
	return true
}
