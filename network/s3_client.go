package network

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

// Constants
const (
	// A Gigabyte!
	GIGABYTE int64 = int64(1024 * 1024 * 1024)

	// Files over 5GB in size must be uploaded via multi-part put.
	S3_LARGE_FILE int64 = int64(5 * GIGABYTE)

	// Chunk size for multipart puts to S3: ~500 MB
	S3_CHUNK_SIZE = int64(500000000)
)

type S3Client struct {
	S3 *s3.S3
}

// Returns an S3Client for the specified region, using
// AWS credentials from the environment. Please keep your AWS
// keys out of the source code repos! Store them somewhere
// else and load them into environment variables AWS_ACCESS_KEY_ID
// and AWS_SECRET_ACCESS_KEY.
func NewS3Client(region aws.Region) (*S3Client, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	s3Client := s3.New(auth, region)
	return &S3Client{S3: s3Client}, nil
}
