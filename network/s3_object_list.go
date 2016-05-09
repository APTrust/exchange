package network

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/aws/session"
	"os"
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
func (s3objectList *S3ObjectList)GetSession() (*session.Session) {
	if s3objectList.session == nil {
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			s3objectList.ErrorMessage = "AWS_ACCESS_KEY_ID and/or " +
				"AWS_SECRET_ACCESS_KEY not set in environment"
			return nil
		}
		creds := credentials.NewEnvCredentials()
		s3objectList.session = session.New(&aws.Config{
			Region:      aws.String(s3objectList.AWSRegion),
			Credentials: creds,
		})
	}
	return s3objectList.session
}

// ObjectList a file to S3. If ErrorMessage == "", the objectList succeeded.
// Check S3ObjectList.Response.Localtion for the item's S3 URL.
func (s3objectList *S3ObjectList) GetList() {
	s3Session := s3objectList.GetSession()
	if s3Session == nil {
		return
	}
	var err error = nil
	service := s3.New(s3Session)
	s3objectList.Response, err = service.ListObjects(s3objectList.ListObjectsInput)
	if err != nil {
		s3objectList.ErrorMessage = err.Error()
	}
}
