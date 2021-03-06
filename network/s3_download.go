package network

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type S3Download struct {
	AWSRegion       string
	BucketName      string
	KeyName         string
	LocalPath       string
	CalculateMd5    bool
	CalculateSha256 bool
	Md5Digest       string
	Sha256Digest    string
	BytesCopied     int64
	ErrorMessage    string

	// The response from S3 for the attempted download.
	// Don't try to read Response.Body, because if this
	// object is non-nil, the response will already have
	// been read and closed.
	Response *s3.GetObjectOutput

	accessKeyId     string
	secretAccessKey string
	session         *session.Session
}

// Sets up a new S3 download. Params:
//
// accessKeyId     - The AWS Access Key Id used to authenticate with AWS.
// secretAccessKey - The AWS secret access key.
// region          - The name of the AWS region to download from.
//                   E.g. us-east-1 (VA), us-west-2 (Oregon), or use
//                   constants.AWSVirginia, constants.AWSOregon
// bucket          - The name of the bucket to download from.
// key             - The name of the file to download.
// localPath       - Path to which to save the downloaded file.
//                   This may be /dev/null in cases where we're
//                   just running a fixity check.
// calculateMd5    - Should we calculate an md5 checksum on
//                   the download?
// calculateSha256 - Should we calculate a sha256 checksum
//                   on the download?
func NewS3Download(accessKeyId, secretAccessKey, region, bucket, key, localPath string, calculateMd5, calculateSha256 bool) *S3Download {
	return &S3Download{
		AWSRegion:       region,
		BucketName:      bucket,
		KeyName:         key,
		LocalPath:       localPath,
		CalculateMd5:    calculateMd5,
		CalculateSha256: calculateSha256,
		accessKeyId:     accessKeyId,
		secretAccessKey: secretAccessKey,
	}
}

// Returns an S3 session for this download.
func (client *S3Download) GetSession() *session.Session {
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
func (client *S3Download) Fetch() {
	_session := client.GetSession()
	if _session == nil {
		return
	}
	service := s3.New(_session)
	if service == nil {
		return
	}
	params := &s3.GetObjectInput{
		Bucket: aws.String(client.BucketName),
		Key:    aws.String(client.KeyName),
	}

	// Try the download several times. On larger files,
	// it's common to get a "connection reset by peer"
	// error, and we'd rather just try again now than
	// requeue the whole job.
	var err error = nil
	for i := 0; i < 5; i++ {
		err = client.tryDownload(service, params)
		if err == nil {
			break
		}
	}
	if err != nil {
		client.ErrorMessage = err.Error()
	}
}

// Tries to download the file from S3. This uses GetObject which
// uses a single HTTP stream, rather than an s3Manager.Downloader,
// which uses multiple streams. We generally have to calculate
// both an md5 and a sha256 checksum on download, and we're choosing
// to write the file and do the checksums all in one pass. The
// s3Manager.Downloader's multiple concurrent connections produce
// faster downloads, but requires a WrterAt interface, which the
// hashing algorithms don't provide. When we're working with
// multi-gigabyte files, we really don't want to have to read them
// again to produce the checksums.
func (client *S3Download) tryDownload(service *s3.S3, params *s3.GetObjectInput) error {
	resp, err := service.GetObject(params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	client.Response = resp

	// Create the download directory and open a file for writing.
	writers := make([]io.Writer, 0)
	if client.LocalPath == os.DevNull {
		writers = append(writers, ioutil.Discard)
	} else {
		err = os.MkdirAll(filepath.Dir(client.LocalPath), 0755)
		if err != nil {
			return err
		}
		outputFile, err := os.Create(client.LocalPath)
		if err != nil {
			return err
		}
		writers = append(writers, outputFile)
		defer outputFile.Close()
	}

	// Create a writer to write the contents to the file,
	// and optionally to pass the bitstream through the
	// md5 and sha256 algorithms while we're at it.
	var multiWriter io.Writer
	var md5Hash hash.Hash
	var sha256Hash hash.Hash
	if client.CalculateMd5 {
		md5Hash = md5.New()
		writers = append(writers, md5Hash)
	}
	if client.CalculateSha256 {
		sha256Hash = sha256.New()
		writers = append(writers, sha256Hash)
	}
	multiWriter = io.MultiWriter(writers...)

	// Copy the file, with several tries. On larger files,
	// we often get a "connection reset by peer" error.
	// Better to retry a few times now than throw this
	// back into the work queue.
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		client.BytesCopied, err = io.Copy(multiWriter, resp.Body)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	// Set the checksums, if needed...
	if client.CalculateMd5 {
		client.Md5Digest = fmt.Sprintf("%x", md5Hash.Sum(nil))
	}
	if client.CalculateSha256 {
		client.Sha256Digest = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	}

	// No errors.
	return nil
}
