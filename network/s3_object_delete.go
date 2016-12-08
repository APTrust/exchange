package network

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3ObjectDelete wraps an S3 client that performs delete
// operations on S3 objects.
type S3ObjectDelete struct {
	AWSRegion    string
	ErrorMessage string

	DeleteObjectsInput *s3.DeleteObjectsInput
	Response           *s3.DeleteObjectsOutput

	session *session.Session
}

// NewS3ObjectDelete returns a new S3ObjectDelete object. Param region
// is the S3 region you want to connect to. Regions are listed at
// http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region,
// and are configured in config settings APTrustS3Region, APTrustGlacierRegion,
// and DPNGlacierRegion. Param bucket is the name of the bucket that contains
// the key you want to delete. Param keys is a list of keys you want to
// delete from that bucket.
func NewS3ObjectDelete(region, bucket string, keys []string) *S3ObjectDelete {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for i := range keys {
		objects[i] = &s3.ObjectIdentifier{
			Key: aws.String(keys[i]),
		}
	}
	deleteObjectsInput := &s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &s3.Delete{
			Objects: objects,
		},
	}
	return &S3ObjectDelete{
		AWSRegion:          region,
		DeleteObjectsInput: deleteObjectsInput,
	}
}

// GetSession returns an S3 session for this object.
func (client *S3ObjectDelete) GetSession() *session.Session {
	if client.session == nil {
		var err error
		if err != nil {
			client.ErrorMessage = err.Error()
		}
		client.session, err = GetS3Session(client.AWSRegion)
	}
	return client.session
}

// DeleteList deletes the list of keys you specified. Check
// s3ObjectDelete.ErrorMessage afterward to see if anything failed. Detailed
// errors will be in s3ObjectDelete.Response.Errors. The S3 Error type is
// defined at  http://docs.aws.amazon.com/sdk-for-go/api/service/s3.html#type-Error
//
// Note that if you try to delete keys that don't exist, you will not
// get an error, and those keys will be shown as deleted in
// s3ObjectDelete.Response.Deleted. That's AWS' design decision.
func (client *S3ObjectDelete) DeleteList() {
	_session := client.GetSession()
	if _session == nil {
		return
	}
	var err error = nil
	service := s3.New(_session)

	client.Response, err = service.DeleteObjects(client.DeleteObjectsInput)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
	for _, err := range client.Response.Errors {
		client.ErrorMessage = fmt.Sprintf("Error deleting key '%s': %s | ", err.Key, err.Message)
	}
}
