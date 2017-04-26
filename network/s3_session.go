package network

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Returns an S3 session for this objectList.
func GetS3Session(awsRegion, accessKeyId, secretAccessKey string) (*session.Session, error) {
	creds := credentials.NewEnvCredentials()
	if accessKeyId != "" && secretAccessKey != "" {
		creds = credentials.NewStaticCredentials(accessKeyId, secretAccessKey, "")
	}
	_session := session.New(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: creds,
	})
	if _session == nil {
		return nil, fmt.Errorf("AWS Session returned nil")
	}
	return _session, nil
}
