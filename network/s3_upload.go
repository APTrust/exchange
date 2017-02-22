package network

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
)

// Typical usage:
//
// upload := NewS3Upload(constants.AWSVirginia, config.PreservationBucket,
//                       "some_uuid", "application/xml")
// upload.AddMetadata("institution", "college.edu")
// upload.AddMetadata("bag", "college.edu/bag")
// upload.AddMetadata("bagpath", "data/file.xml")
// upload.AddMetadata("md5", "12345678")
// upload.AddMetadata("sha256", "87654321")
// reader, err := os.Open("/path/to/file.txt")
// if err != nil {
//    ... whatever ...
// }
// defer reader.Close()
// upload.Send(reader)
// if upload.ErrorMessage != "" {
//    ... do something ...
// }
// urlOfNewItem := upload.Response.Location
//
type S3Upload struct {
	AWSRegion    string
	ErrorMessage string
	UploadInput  *s3manager.UploadInput
	Response     *s3manager.UploadOutput
	session      *session.Session
	partSize     int64
	concurrency  int
}

// Creates a new S3 upload object using the s3Manager.Uploader described at
// https://godoc.org/github.com/aws/aws-sdk-go/service/s3/s3manager#Uploader
//
// The uploader uses concurrent goroutines for speed, and is smart enough
// to handle both normal and multi-part uploads. It also cleans up stray
// file parts in cases where a multi-part upload fails.
//
// Params:
//
// region     - The name of the AWS region to download from.
//              E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//              constants.AWSVirginia, constants.AWSOregon
// bucket     - The name of the bucket to download from.
// key        - The name of the file to download.
// contentType - A standard Content-Type header, like text/html.
func NewS3Upload(region, bucket, key, contentType string) *S3Upload {
	uploadInput := &s3manager.UploadInput{
		Bucket:      &bucket,
		Key:         &key,
		ContentType: &contentType,
	}
	uploadInput.Metadata = make(map[string]*string)
	return &S3Upload{
		AWSRegion:   region,
		UploadInput: uploadInput,
	}
}

// Returns an S3 session for this upload.
func (client *S3Upload) GetSession() *session.Session {
	if client.session == nil {
		var err error
		if err != nil {
			client.ErrorMessage = err.Error()
		}
		client.session, err = GetS3Session(client.AWSRegion)
	}
	return client.session
}

// Adds metadata to the upload. We should be adding the following:
//
// x-amz-meta-institution
// x-amz-meta-bag
// x-amz-meta-bagpath
// x-amz-meta-md5
// x-amz-meta-sha256
func (client *S3Upload) AddMetadata(key, value string) {
	client.UploadInput.Metadata[key] = &value
}

// Upload a file to S3. If ErrorMessage == "", the upload succeeded.
// Check S3Upload.Response.Localtion for the item's S3 URL.
// Caller is responsible for closing the reader.
//
// If you're sending anything larger than constants.S3LargeFileSize,
// don't pass a tarReader into this function. Pass in a File, or
// something that supports Seek() and ReadAt(). Otherwise, Amazon's
// s3 upload library will be very stupid and read ALL of the buffered
// chunks into memory at once. (Seriously, what's the point of buffering
// and chunking if you do that?) That causes the worker process to
// crash due to lack of memory. (Esp. when we're dealing with 1TB files.)
// See apt_storer for an example.
func (client *S3Upload) Send(reader io.Reader) {
	_session := client.GetSession()
	if _session == nil {
		return
	}
	uploader := s3manager.NewUploader(_session)
	client.UploadInput.Body = reader
	var err error
	client.Response, err = uploader.Upload(client.UploadInput)
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}

func (client *S3Upload) PartSize() int64 {
	return client.partSize
}

func (client *S3Upload) Concurrency() int {
	return client.concurrency
}
