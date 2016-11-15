package network

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

// Returns an S3 session for this objectList.
func GetS3Session(awsRegion string) (*session.Session, error) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and/or " +
			"AWS_SECRET_ACCESS_KEY not set in environment")
	}
	creds := credentials.NewEnvCredentials()
	_session := session.New(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: creds,
	})
	if _session == nil {
		return nil, fmt.Errorf("AWS Session returned nil")
	}
	return _session, nil
}
