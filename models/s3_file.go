package models

import (
	"fmt"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/s3util"
	"github.com/crowdmob/goamz/s3"
	"time"
)


// S3File contains information about the S3 file we're
// trying to process from an intake bucket. BucketName
// and Key are the S3 bucket name and key. AttemptNumber
// describes whether this is the 1st, 2nd, 3rd,
// etc. attempt to process this file.
type S3File struct {
	// The name of the S3 bucket that holds this key.
	BucketName string

	// The S3 Key, with object name, size, etag, last modified, etc.
	//
	// TODO: On delete jobs, you'll need to fetch the key object from S3.
	Key        s3.Key

	// If we attempted to delete this file from S3 and got an error
	// message, that message will be stored here. This field is only
	// relevant in the context of the file delete worker.
	ErrorMessage string

	// The date and time at which the key/file was successfully deleted
	// from S3. If this is zero time, file was not deleted. If it's
	// any other time, delete succeeded. This field is only relevant
	// in the context of the delete worker.
	DeletedAt time.Time

	// Flag to indicate whether we skipped the delete
	// operation because config.DeleteOnSuccess == false.
	// This field is only relevant in the context of the delete worker.
	DeleteSkippedPerConfig bool
}

func NewS3FileWithKey(bucketName string, key s3.Key) (*S3File) {
	return &S3File{
		BucketName: bucketName,
		Key: key,
	}
}

func NewS3FileWithName(bucketName, keyName string) (*S3File) {
	return &S3File{
		BucketName: bucketName,
		Key: s3.Key{ Key: keyName },
	}
}


// Returns the object identifier that will identify this bag
// in fedora. That's the institution identifier, followed by
// a slash and the tar file name, minus the .tar extension
// and the ".bag1of12" multipart extension. So for BucketName
// "aptrust.receiving.unc.edu" and Key.Key "nc_bag.b001.of030.tar",
// this would return "unc.edu/nc_bag"
func (s3File *S3File) ObjectName() (string, error) {
	institution := util.OwnerOf(s3File.BucketName)
	cleanBagName, err := util.CleanBagName(s3File.Key.Key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", institution, cleanBagName), nil
}

// The name of the owning institution, followed by a slash, followed
// by the name of the tar file. This differs from the ObjectName,
// because it will have the .tar or bag.001.of030.tar suffix.
func (s3File *S3File) BagName() (string) {
	return fmt.Sprintf("%s/%s", util.OwnerOf(s3File.BucketName), s3File.Key.Key)
}

// Returns true if we attempted to delete this file.
func (s3File *S3File) DeleteAttempted() bool {
	return s3File.ErrorMessage != "" || s3File.DeletedAt.IsZero() == false
}

// Returns true of the S3 key is complete. In some cases, we only
// have the Key.Key and we have to fetch the rest from S3.
func (s3File *S3File) KeyIsComplete() bool {
	return s3util.KeyIsComplete(s3File.Key)
}
