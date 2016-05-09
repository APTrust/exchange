package network

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/aws/aws-sdk-go/aws/session"
	"os"
)

// Typical usage:
//
// upload := NewS3Upload(constants.AWSVirginia, config.PreservationBucket,
//                       "some_uuid", "/mnt/apt/data/college.edu/bag/data/file.xml",
//                       "application/xml")
// upload.AddMetadata("institution", "college.edu")
// upload.AddMetadata("bag", "college.edu/bag")
// upload.AddMetadata("bagpath", "data/file.xml")
// upload.AddMetadata("md5", "12345678")
// upload.AddMetadata("sha256", "87654321")
// upload.Send()
// if upload.ErrorMessage != "" {
//    ... do something ...
// }
// urlOfNewItem := upload.Response.Location
//
type S3Upload struct {
	AWSRegion       string
	LocalPath       string
	ErrorMessage    string
	UploadInput     *s3manager.UploadInput
	Response        *s3manager.UploadOutput

	session         *session.Session
}

// Creates a new S3 upload object. Params:
//
// region     - The name of the AWS region to download from.
//              E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//              constants.AWSVirginia, constants.AWSOregon
// bucket     - The name of the bucket to download from.
// key        - The name of the file to download.
// localPath  - Path to which to save the downloaded file.
//              This may be /dev/null in cases where we're
//              just running a fixity check.
// contentType - A standard Content-Type header, like text/html.
func NewS3Upload(region, bucket, key, localPath, contentType string) (*S3Upload) {
	uploadInput := &s3manager.UploadInput{
		Bucket: &bucket,
		Key: &key,
		ContentType: &contentType,
	}
	uploadInput.Metadata = make(map[string]*string)
	return &S3Upload{
		AWSRegion: region,
		LocalPath: localPath,
		UploadInput: uploadInput,
	}
}

// Returns an S3 session for this upload.
func (s3upload *S3Upload)GetSession() (*session.Session) {
	if s3upload.session == nil {
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			s3upload.ErrorMessage = "AWS_ACCESS_KEY_ID and/or " +
				"AWS_SECRET_ACCESS_KEY not set in environment"
			return nil
		}
		creds := credentials.NewEnvCredentials()
		s3upload.session = session.New(&aws.Config{
			Region:      aws.String(s3upload.AWSRegion),
			Credentials: creds,
		})
	}
	return s3upload.session
}

// Adds metadata to the upload. We should be adding the following:
//
// x-amz-meta-institution
// x-amz-meta-bag
// x-amz-meta-bagpath
// x-amz-meta-md5
// x-amz-meta-sha256
func (s3upload *S3Upload) AddMetadata(key, value string) {
	s3upload.UploadInput.Metadata[key] = &value
}

// Upload a file to S3. If ErrorMessage == "", the upload succeeded.
// Check S3Upload.Response.Localtion for the item's S3 URL.
func (s3upload *S3Upload) Send() {
	file, err := os.Open(s3upload.LocalPath)
    if err != nil {
        s3upload.ErrorMessage = err.Error()
		return
    }
	defer file.Close()
	s3upload.UploadInput.Body = file
    uploader := s3manager.NewUploader(s3upload.GetSession())
	uploader.LeavePartsOnError = false // we have to pay for abandoned parts
    s3upload.Response, err = uploader.Upload(s3upload.UploadInput)
    if err != nil {
        s3upload.ErrorMessage = err.Error()
    }
}
