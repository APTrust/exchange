package network

import (
	//	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/util"
	//	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	//	"strings"
	//	"time"
)

type GlacierJobStatus struct {
	AWSRegion       string
	BucketName      string
	accessKeyId     string
	secretAccessKey string
	session         *session.Session
}

func NewGlacierJobStatus(accessKeyId, secretAccessKey, region, bucket string) *GlacierJobStatus {
	return &GlacierJobStatus{
		AWSRegion:       region,
		BucketName:      bucket,
		accessKeyId:     accessKeyId,
		secretAccessKey: secretAccessKey,
	}
}

// Returns an S3 session for this head request.
func (client *GlacierJobStatus) GetSession() (*session.Session, error) {
	var err error
	if client.session == nil {
		client.session, err = GetS3Session(client.AWSRegion,
			client.accessKeyId, client.secretAccessKey)
	}
	return client.session, err
}

func (client *GlacierJobStatus) GetStatus(jobId string) (*glacier.JobDescription, error) {
	return nil, nil
}
