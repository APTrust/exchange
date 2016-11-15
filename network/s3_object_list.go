package network

import (
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/aws/session"
)

type S3ObjectList struct {
	AWSRegion        string
	ErrorMessage     string

	ListObjectsInput *s3.ListObjectsInput
	Response         *s3.ListObjectsOutput

	session          *session.Session
}

func NewS3ObjectList(region, bucket string, maxKeys int64) (*S3ObjectList) {
	listObjectsInput :=  &s3.ListObjectsInput{
		Bucket: &bucket,
		MaxKeys: &maxKeys,
	}
	return &S3ObjectList{
		AWSRegion: region,
		ListObjectsInput: listObjectsInput,
	}
}

// Returns an S3 session for this objectList.
func (client *S3ObjectList)GetSession() (*session.Session) {
	if client.session == nil {
		var err error
		if err != nil {
			client.ErrorMessage = err.Error()
		}
		client.session, err = GetS3Session(client.AWSRegion)
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

	if client.Response != nil && *client.Response.IsTruncated {
		client.ListObjectsInput.Marker = client.Response.NextMarker
	}
	if prefix != "" {
		client.ListObjectsInput.Prefix = &prefix
	}
	client.Response, err = service.ListObjects(client.ListObjectsInput)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}
