package network

import (
	"github.com/crowdmob/goamz/s3"
)

type S3Download struct {
	BucketName      string
	KeyName         string
	S3Key           *s3.Key
	LocalPath       string
	CalculateMd5    bool
	CalculateSha256 bool
	Md5Digest       string
	Sha256Digest    string
	BytesCopied     int64
	ErrorMessage    string
}

// Sets up a new S3 download. Params:
//
// bucketName - The name of the bucket to download from.
// keyName    - The name of the file to download.
// localPath  - Path to which to save the downloaded file.
//              This may be /dev/null in cases where we're
//              just running a fixity check.
// calculateMd5 - Should we calculate an md5 checksum on
//              the download?
// calculateSha256 - Should we calculate a sha256 checksum
//              on the download?
func NewS3Download(bucketName, keyName, localPath string, calculateMd5, calculateSha256 bool) (*S3Download) {
	return &S3Download{
		BucketName: bucketName,
		KeyName: keyName,
		S3Key: nil,
		LocalPath: localPath,
		CalculateMd5: calculateMd5,
		CalculateSha256: calculateSha256,
		Md5Digest: "",
		Sha256Digest: "",
	}
}
