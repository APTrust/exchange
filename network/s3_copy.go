package network

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/url"
)

type S3Copy struct {
	AWSRegion         string
	SourceBucket      string
	SourceKey         string
	DestinationBucket string
	DestinationKey    string
	ErrorMessage      string
	Response          *s3.CopyObjectOutput
	accessKeyId       string
	secretAccessKey   string
	session           *session.Session
}

// Sets up a new S3Copy object. Params:
//
// accessKeyId     - The AWS Access Key Id used to authenticate with AWS.
// secretAccessKey - The AWS secret access key.
// region          - The name of the AWS region where the source file is stored.
//                   E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//                   constants.AWSVirginia, constants.AWSOregon
// sourceBucket    - The name of the bucket to copy from.
// sourceKey       - The name/key S3 object to be copied.
// destinationBucket - The name of the bucket to copy to.
// destinationKey    - The name/key of the S3 object in the destination bucket.
func NewS3Copy(accessKeyId, secretAccessKey, region, sourceBucket, sourceKey, destinationBucket, destinationKey string) *S3Copy {
	return &S3Copy{
		AWSRegion:         region,
		SourceBucket:      sourceBucket,
		SourceKey:         sourceKey,
		DestinationBucket: destinationBucket,
		DestinationKey:    destinationKey,
		accessKeyId:       accessKeyId,
		secretAccessKey:   secretAccessKey,
	}
}

// AWS docs say CopySource must be URL encoded.
func (client *S3Copy) CopySource() string {
	return fmt.Sprintf("%s/%s", url.PathEscape(client.SourceBucket), url.PathEscape(client.SourceKey))
}

// Returns an S3 session for this copy operation.
func (client *S3Copy) GetSession() *session.Session {
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

// Fetch the file from S3.
func (client *S3Copy) Copy() {
	client.Response = nil
	_session := client.GetSession()
	if _session == nil {
		return
	}
	service := s3.New(_session)
	if service == nil {
		return
	}
	copyObjectInput := &s3.CopyObjectInput{
		CopySource: aws.String(client.CopySource()),
		Bucket:     aws.String(client.DestinationBucket),
		Key:        aws.String(client.DestinationKey),
	}
	var err error
	client.Response, err = service.CopyObject(copyObjectInput)
	if err != nil {
		client.ErrorMessage = err.Error()
		return
	}
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(client.DestinationBucket),
		Key:    aws.String(client.DestinationKey),
	}
	err = service.WaitUntilObjectExists(headObjectInput)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}
