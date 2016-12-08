package s3util

import (
	"github.com/crowdmob/goamz/s3"
)

// KeyIsComplete returns true if S3 Key key appears to be
// complete. In some cases, we have only the key name and will
// need to fetch the result of the key info from S3.
func KeyIsComplete(key s3.Key) bool {
	return key.Key != "" && key.Size != 0 && key.ETag != ""
}
