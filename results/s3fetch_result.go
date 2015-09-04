package results

import (
	"github.com/APTrust/exchange/util/s3util"
	"github.com/crowdmob/goamz/s3"
)

// S3FetchResult descibes the results of fetching a bag from S3
// and verification of that bag.
type S3FetchResult struct {
	BucketName    string
	Key           s3.Key
	LocalFile     string
	RemoteMd5     string
	LocalMd5      string
	Md5Verified   bool
	Md5Verifiable bool
	Summary       Summary
}

func NewS3FetchResultWithKey(bucketName string, key s3.Key) (*S3FetchResult) {
	return &S3FetchResult{
		BucketName: bucketName,
		Key: key,
		Summary: NewSummary(),
	}
}

func NewS3FetchResultWithName(bucketName, keyName string) (*S3FetchResult) {
	return &S3FetchResult{
		BucketName: bucketName,
		Key: s3.Key{ Key: keyName },
		Summary: NewSummary(),
	}
}

func (result *S3FetchResult) KeyIsComplete() bool {
	return s3util.KeyIsComplete(result.Key)
}
