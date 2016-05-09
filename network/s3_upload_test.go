package network_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewS3Upload(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"file/does/not/exist.tar",
		"application/tar",
	)
	assert.Equal(t, testBucket, *upload.UploadInput.Bucket)
	assert.Equal(t, "s3_upload_test.tar", *upload.UploadInput.Key)
	assert.Equal(t, "application/tar", *upload.UploadInput.ContentType)
	assert.Equal(t, constants.AWSVirginia, upload.AWSRegion)
	assert.Equal(t, "file/does/not/exist.tar", upload.LocalPath)
}

func TestS3UploadAddMetadata(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"file/does/not/exist.tar",
		"application/tar",
	)
	upload.AddMetadata("institution", "test.edu")
	upload.AddMetadata("bag", "test.edu/s3_upload_test")
	upload.AddMetadata("bagpath", "data/test/path.xml")
	upload.AddMetadata("md5", "FAKE-TEST-MD5")
	upload.AddMetadata("sha256", "FAKE-TEST-SHA256")
	assert.Equal(t, "test.edu", *upload.UploadInput.Metadata["institution"])
	assert.Equal(t, "test.edu/s3_upload_test", *upload.UploadInput.Metadata["bag"])
	assert.Equal(t, "data/test/path.xml", *upload.UploadInput.Metadata["bagpath"])
	assert.Equal(t, "FAKE-TEST-MD5", *upload.UploadInput.Metadata["md5"])
	assert.Equal(t, "FAKE-TEST-SHA256", *upload.UploadInput.Metadata["sha256"])
}

func TestS3UploadBadFile(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"file/does/not/exist.tar",
		"application/tar",
	)
	upload.Send()
	assert.Equal(t, "open file/does/not/exist.tar: no such file or directory", upload.ErrorMessage)
}

func TestS3UploadGoodFile(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"../testdata/virginia.edu.uva-lib_2278801.tar",
		"application/tar",
	)
	upload.AddMetadata("institution", "test.edu")
	upload.AddMetadata("bag", "test.edu/s3_upload_test")
	upload.AddMetadata("bagpath", "data/test/path.xml")
	upload.AddMetadata("md5", "FAKE-TEST-MD5")
	upload.AddMetadata("sha256", "FAKE-TEST-SHA256")
	upload.Send()
	assert.Equal(t, "", upload.ErrorMessage)
}
