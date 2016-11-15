package network

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3ObjectDelete struct {
	AWSRegion    string
	ErrorMessage string

	DeleteObjectsInput *s3.DeleteObjectsInput
	Response           *s3.DeleteObjectsOutput

	session *session.Session
}

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

// Returns an S3 session for this object.
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

// Deletes the list of keys you specified. Check s3ObjectDelete.ErrorMessage
// afterward to see if anything failed. Detailed errors will be in
// s3ObjectDelete.Response.Errors. The S3 Error type is defined here:
// http://docs.aws.amazon.com/sdk-for-go/api/service/s3.html#type-Error
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
