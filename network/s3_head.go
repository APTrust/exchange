package network

import (
	dpn_models "github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"time"
)

type S3Head struct {
	AWSRegion       string
	BucketName      string
	ErrorMessage    string
	Response        *s3.HeadObjectOutput
	input           *s3.HeadObjectInput
	session         *session.Session
	accessKeyId     string
	secretAccessKey string
}

// Contains info parsed from x-amz-restore header,
// if that header is present. The header will only exist
// if we recently requested the item be retrieved from
// Glacier into S3.
type RestoreRequestInfo struct {
	RequestInProgress bool
	RequestIsComplete bool
	S3ExpiryDate      time.Time
}

// Sets up a new S3 head request. Params:
//
// accessKeyId     - The AWS Access Key Id used to authenticate with AWS.
// secretAccessKey - The AWS secret access key.
// region     - The name of the AWS region to download from.
//              E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//              constants.AWSVirginia, constants.AWSOregon
// bucket     - The name of the bucket to download from.
func NewS3Head(accessKeyId, secretAccessKey, region, bucket string) *S3Head {
	return &S3Head{
		AWSRegion:       region,
		BucketName:      bucket,
		accessKeyId:     accessKeyId,
		secretAccessKey: secretAccessKey,
	}
}

// Returns an S3 session for this head request.
func (client *S3Head) GetSession() *session.Session {
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

// Head sends a HEAD request to S3 for the specified key.
// After calling this, check client.ErrorMessage and client.Response,
// which contains a HeadObjectOutput struct. See the docs here:
// https://godoc.org/github.com/aws/aws-sdk-go/service/s3#HeadObjectOutput
//
// The most relevant items for us in the HeadObjectOutput struct are
// ContentLength, ContentType, LastModified, Metadata, and VersionId.
func (client *S3Head) Head(key string) {
	client.Response = nil
	client.ErrorMessage = ""
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
	client.input = params
	request, response := service.HeadObjectRequest(params)
	err := request.Send()
	if err != nil {
		client.ErrorMessage = err.Error()
		return
	}
	client.Response = response
}

// GetHeaderMetadata
func (client *S3Head) GetHeaderMetadata(key string) string {
	resp := client.Response
	value := ""
	if resp != nil && resp.Metadata != nil {
		valPointer := resp.Metadata[key]
		if valPointer != nil {
			value = *valPointer
		}
	}
	return value
}

func (client *S3Head) GetRestoreRequestInfo() (*RestoreRequestInfo, error) {
	restoreRequestInfo := &RestoreRequestInfo{
		RequestInProgress: false,
		RequestIsComplete: false,
		S3ExpiryDate:      time.Time{},
	}
	if client.Response == nil || util.PointerToString(client.Response.Restore) == "" {
		return restoreRequestInfo, nil
	}

	restoreInfoParts := strings.SplitN(util.PointerToString(client.Response.Restore), ",", 2)

	// The expiry section of the header may or may not exist.
	if len(restoreInfoParts) > 1 {
		expiry := strings.SplitN(restoreInfoParts[1], "=", 2)
		if len(expiry) > 1 {
			// dateString format is "Fri, 23 Dec 2012 00:00:00 GMT"
			// We need to remove the quotes.
			// See https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
			dateString := strings.Replace(expiry[1], "\"", "", -1)
			fileExpires, err := time.Parse(time.RFC1123, dateString)
			if err != nil {
				return nil, err
			}
			restoreRequestInfo.RequestIsComplete = true
			restoreRequestInfo.S3ExpiryDate = fileExpires
		}
	}

	// If the header is present, the ongoing-request section should be present.
	if len(restoreInfoParts) > 0 {
		ongoing := strings.SplitN(restoreInfoParts[0], "=", 2)
		if len(ongoing) > 1 && strings.Replace(ongoing[1], "\"", "", -1) == "true" {
			restoreRequestInfo.RequestInProgress = true
		}
	}
	return restoreRequestInfo, nil
}

func (client *S3Head) StoredFile() *models.StoredFile {
	resp := client.Response
	if resp == nil || client.input == nil {
		return nil
	}
	now := time.Now().UTC()
	storedFile := &models.StoredFile{
		Key:          util.PointerToString(client.input.Key),
		ETag:         strings.Replace(*resp.ETag, "\"", "", -1),
		LastModified: *resp.LastModified,
		Size:         *resp.ContentLength,
		Bucket:       util.PointerToString(client.input.Bucket),
		ContentType:  util.PointerToString(resp.ContentType),
		Institution:  client.GetHeaderMetadata("Institution"),
		BagName:      client.GetHeaderMetadata("Bag"),
		PathInBag:    client.GetHeaderMetadata("Bagpath"),
		Md5:          client.GetHeaderMetadata("Md5"),
		Sha256:       client.GetHeaderMetadata("Sha256"),
		LastSeenAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return storedFile
}

func (client *S3Head) DPNStoredFile() *dpn_models.DPNStoredFile {
	resp := client.Response
	if resp == nil || client.input == nil {
		return nil
	}
	now := time.Now().UTC()
	storedFile := &dpn_models.DPNStoredFile{
		Key:          util.PointerToString(client.input.Key),
		ETag:         strings.Replace(*resp.ETag, "\"", "", -1),
		LastModified: *resp.LastModified,
		Size:         *resp.ContentLength,
		Bucket:       util.PointerToString(client.input.Bucket),
		ContentType:  util.PointerToString(resp.ContentType),
		Member:       client.GetHeaderMetadata("Member"),
		FromNode:     client.GetHeaderMetadata("From_node"),
		TransferId:   client.GetHeaderMetadata("Transfer_id"),
		LocalId:      client.GetHeaderMetadata("Local_id"),
		Version:      client.GetHeaderMetadata("Version"),
		LastSeenAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return storedFile
}
