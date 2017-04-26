package network

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

// Returns an S3 session for this objectList.
func GetS3Session(awsRegion, accessKeyId, secretAccessKey string) (*session.Session, error) {
	testsAreRunning := flag.Lookup("test.v") != nil
	if !testsAreRunning && (os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "") {
		panic("AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY not set in environment")
	}
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
