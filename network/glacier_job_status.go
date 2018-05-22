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
	accessKeyId     string
	secretAccessKey string
	session         *session.Session
}

func NewGlacierJobStatus(accessKeyId, secretAccessKey, region string) *GlacierJobStatus {
	return &GlacierJobStatus{
		AWSRegion:       region,
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

func (client *GlacierJobStatus) GetStatus(vaultName, jobId string) (*glacier.JobDescription, error) {
	_session, err := client.GetSession()
	if err != nil {
		return nil, err
	}
	// Note: AWS docs say setting AccountId to "-" tells
	// Glacier to use the account id associated with
	// the credentials we supplied with accessKeyId and secretAccessKey.
	// https://docs.aws.amazon.com/sdk-for-go/api/service/glacier/#DescribeJobInput
	accountId := "-"
	glacierClient := glacier.New(_session)
	input := &glacier.DescribeJobInput{
		AccountId: &accountId,
		JobId:     &jobId,
		VaultName: &vaultName,
	}
	return glacierClient.DescribeJob(input)
}
