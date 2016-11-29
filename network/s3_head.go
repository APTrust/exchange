package network

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Head struct {
	AWSRegion    string
	BucketName   string
	ErrorMessage string
	Response     *s3.HeadObjectOutput
	session      *session.Session
}

// Sets up a new S3 head request. Params:
//
// region     - The name of the AWS region to download from.
//              E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//              constants.AWSVirginia, constants.AWSOregon
// bucket     - The name of the bucket to download from.
func NewS3Head(region, bucket string) *S3Head {
	return &S3Head{
		AWSRegion:  region,
		BucketName: bucket,
	}
}

// Returns an S3 session for this head request.
func (client *S3Head) GetSession() *session.Session {
	if client.session == nil {
		var err error
		if err != nil {
			client.ErrorMessage = err.Error()
		}
		client.session, err = GetS3Session(client.AWSRegion)
	}
	return client.session
}

// Head sends a HEAD request to S3 for the specified key.
// After calling this, check client.ErrorMessage and client.Response,
// which contains a HeadObjectOutput struct. See the docs here:
// https://godoc.org/github.com/aws/aws-sdk-go/service/s3#HeadObjectOutput
//
// The most relevant items for us in the HeadObjectOutput struct are
// ContentLength, ContentType, LastModified, Metadata, and VersionId.
func (client *S3Head) Head(key string) {
	_session := client.GetSession()
	if _session == nil {
		return
	}
	service := s3.New(_session)
	if service == nil {
		return
	}
	// Note that we may also someday set VersionId on HeadObjectInput
	// to retrieve specific versions of a file, depending on how DPN
	// and APTrust choose to implement versioning. As of late 2016,
	// we do not use the versioning features provided by S3 and Glacier.
	params := &s3.HeadObjectInput{
		Bucket: aws.String(client.BucketName),
		Key:    aws.String(key),
	}
	request, response := service.HeadObjectRequest(params)
	err := request.Send()
	if err != nil {
		client.ErrorMessage = err.Error()
	}
	client.Response = response
}
