package network

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3ObjectList struct {
	AWSRegion    string
	ErrorMessage string

	ListObjectsInput *s3.ListObjectsInput
	Response         *s3.ListObjectsOutput

	session         *session.Session
	accessKeyId     string
	secretAccessKey string
}

// NewS3ObjectList returns an object that will list items in an
// S3 bucket.
//
// accessKeyId     - The AWS Access Key Id used to authenticate with AWS.
// secretAccessKey - The AWS secret access key.
// region - The S3 region to connect to.
// bucket - The bucket to list
// maxKeys - The maximum number of items to list
func NewS3ObjectList(accessKeyId, secretAccessKey, region, bucket string, maxKeys int64) *S3ObjectList {
	listObjectsInput := &s3.ListObjectsInput{
		Bucket:  &bucket,
		MaxKeys: &maxKeys,
	}
	return &S3ObjectList{
		AWSRegion:        region,
		ListObjectsInput: listObjectsInput,
		accessKeyId:      accessKeyId,
		secretAccessKey:  secretAccessKey,
	}
}

// Returns an S3 session for this objectList.
func (client *S3ObjectList) GetSession() *session.Session {
	if client.session == nil {
		var err error
		client.session, err = GetS3Session(client.AWSRegion,
			client.accessKeyId, client.secretAccessKey)
		if err != nil {
			client.ErrorMessage = err.Error()
		}
	}
	return client.session
}

// Returns a list of objects from this S3 bucket.
// If param prefix is not an empty string, this returns
// only keys with the specified prefix.
// Check *s3ObjectList.Response.IsTruncated to see if
// you got the complete list. If not, keep calling
// GetList until IsTruncated == false.
func (client *S3ObjectList) GetList(prefix string) {
	_session := client.GetSession()
	if _session == nil {
		return
	}
	var err error = nil
	service := s3.New(_session)

	if client.Response != nil && client.Response.IsTruncated != nil && *client.Response.IsTruncated {
		if prefix == "" {
			// See doc for ListObjectOutput.NextMarker at
			// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
			// If there's no prefix in the initial request, we have
			// to set this ourselves.
			lastKey := client.Response.Contents[len(client.Response.Contents)-1].Key
			client.ListObjectsInput.Marker = lastKey
		} else {
			client.ListObjectsInput.Marker = client.Response.NextMarker
		}
	}
	if prefix != "" {
		client.ListObjectsInput.Prefix = &prefix
	}
	client.Response, err = service.ListObjects(client.ListObjectsInput)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}
