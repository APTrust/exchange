package network

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

type S3Restore struct {
	AWSRegion                         string
	BucketName                        string
	KeyName                           string
	Tier                              string
	Days                              int64
	ErrorMessage                      string
	Response                          *s3.RestoreObjectOutput
	RestoreAlreadyInProgress          bool
	AlreadyInActiveTier               bool
	RequestRejectedServiceUnavailable bool
	session                           *session.Session
	accessKeyId                       string
	secretAccessKey                   string

	// TestURL is the URL of a mock S3 server
	// for use in unit tests only.
	TestURL string
}

// Sets up as S3 restore request, which is for S3 items
// that have been archived into Glacier. Normal S3 items
// do not need restore requests. You can just use s3_download
// to get them directly.
//
// s3_restore simply initiates a restore request, which
// generally takes several hours to complete. Check the S3
// bucket periodically to see if the item has been restored.
//
// Params:
//
// accessKeyId     - The AWS Access Key Id used to authenticate with AWS.
// secretAccessKey - The AWS secret access key.
// region     - The name of the AWS region to download from.
//              E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//              constants.AWSVirginia, constants.AWSOregon
// bucket     - The name of the bucket to download from.
// key        - The name of the file to download.
// tier       - The Glacier retrieval tier. Values are "Expedited",
//              "Standard" and "Bulk". We almost always want "Standard".
//              "Expedited" is expensive, and "Bulk" doesn't really
//              saves us much.
// days       - The number of days to leave the restored item in
//              the S3 bucket after retrieving it.
func NewS3Restore(accessKeyId, secretAccessKey, region, bucket, key, tier string, days int64) *S3Restore {
	return &S3Restore{
		AWSRegion:  region,
		BucketName: bucket,
		KeyName:    key,
		Tier:       tier,
		Days:       days,
		RestoreAlreadyInProgress:          false,
		AlreadyInActiveTier:               false,
		RequestRejectedServiceUnavailable: false,
		accessKeyId:                       accessKeyId,
		secretAccessKey:                   secretAccessKey,
	}
}

// Returns an S3 session for this restore request.
func (client *S3Restore) GetSession() *session.Session {
	if client.session == nil {
		if client.TestURL == "" {
			client.getSession()
		} else {
			client.getTestSession()
		}
	}
	return client.session
}

func (client *S3Restore) getSession() {
	var err error
	client.session, err = GetS3Session(client.AWSRegion,
		client.accessKeyId, client.secretAccessKey)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}

func (client *S3Restore) getTestSession() {
	creds := credentials.NewStaticCredentials(client.accessKeyId, client.secretAccessKey, "")
	client.session = session.New(&aws.Config{
		Region:      aws.String(client.AWSRegion),
		Credentials: creds,
		Endpoint:    &client.TestURL,
	})
	if client.session == nil {
		client.ErrorMessage = "AWS Session (with TestURL) returned nil"
	}
}

// Restore the archived file from Glacier to S3.
func (client *S3Restore) Restore() {
	client.Response = nil
	client.ErrorMessage = ""
	client.RestoreAlreadyInProgress = false
	_session := client.GetSession()
	if _session == nil {
		return
	}
	service := s3.New(_session)
	if service == nil {
		return
	}
	params := &s3.RestoreObjectInput{
		Bucket: aws.String(client.BucketName),
		Key:    aws.String(client.KeyName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(client.Days),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String(client.Tier),
			},
		},
	}
	resp, err := service.RestoreObject(params)
	client.Response = resp
	client.checkError(err)
}

func (client *S3Restore) checkError(err error) {
	if err == nil {
		return
	}

	// The first two conditions, indicating that the
	// item is already in the active tier, or the
	// restore is currently in progress, should happen
	// often. We don't want to treat these as errors,
	// even though the S3 service may indicate them
	// as such. We do want to let the caller know what
	// state the restore request is in.
	//
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOSTrestore.html#RESTObjectPOSTrestore-responses
	if client.isActiveTierError(err) {
		client.AlreadyInActiveTier = true
	} else if client.isRestoreInProgressError(err) {
		client.RestoreAlreadyInProgress = true
	} else if client.isServiceUnavailableError(err) {
		client.RequestRejectedServiceUnavailable = true
	} else {
		// This is not a normal or expected condition,
		// so we do consider this an error.
		client.ErrorMessage = err.Error()
	}
}

// We cannot test this with our unit test because AlreadyInActiveTier
// is indicated by an HTTP 200 status code, and s3.RestoreObjectOutput
// gives us no access to the underlying HTTP response or its status code.
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOSTrestore.html#RESTObjectPOSTrestore-responses
func (client *S3Restore) isActiveTierError(err error) bool {
	isActive := false
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == s3.ErrCodeObjectAlreadyInActiveTierError {
			isActive = true
		}
	}
	return isActive
}

func (client *S3Restore) isRestoreInProgressError(err error) bool {
	isInProgress := false
	lcErrorMessage := strings.ToLower(err.Error())
	if strings.Contains(lcErrorMessage, "restorealreadyinprogress") ||
		strings.Contains(lcErrorMessage, "conflict") ||
		strings.Contains(lcErrorMessage, "status code: 409") {
		isInProgress = true
	}
	return isInProgress
}

func (client *S3Restore) isServiceUnavailableError(err error) bool {
	is503Response := false
	lcErrorMessage := strings.ToLower(err.Error())
	if strings.Contains(lcErrorMessage, "serviceunavailable") ||
		strings.Contains(lcErrorMessage, "status code: 503") {
		is503Response = true
	}
	return is503Response
}

// RequestAccepted returns true if the restore request was
// accepted, or if it is already in progress or has already
// been completed.
func (client *S3Restore) RequestAccepted() bool {
	return (client.Response != nil &&
		client.ErrorMessage == "" &&
		client.RequestRejectedServiceUnavailable == false)
}
