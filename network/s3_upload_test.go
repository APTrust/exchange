package network_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestNewS3Upload(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"application/tar",
	)
	assert.Equal(t, testBucket, *upload.UploadInput.Bucket)
	assert.Equal(t, "s3_upload_test.tar", *upload.UploadInput.Key)
	assert.Equal(t, "application/tar", *upload.UploadInput.ContentType)
	assert.Equal(t, constants.AWSVirginia, upload.AWSRegion)
}

func TestS3UploadAddMetadata(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
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
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"application/tar",
	)
	file, _ := os.Open("file/does/not/exist.tar")
	upload.Send(file)
	assert.True(t, strings.Contains(upload.ErrorMessage, "invalid argument"))
}

func TestS3UploadGoodFile(t *testing.T) {
	if !canTestS3() {
		return
	}
	upload := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		"s3_upload_test.tar",
		"application/tar",
	)
	upload.AddMetadata("institution", "test.edu")
	upload.AddMetadata("bag", "test.edu/s3_upload_test")
	upload.AddMetadata("bagpath", "data/test/path.xml")
	upload.AddMetadata("md5", "FAKE-TEST-MD5")
	upload.AddMetadata("sha256", "FAKE-TEST-SHA256")
	file, err := os.Open("../testdata/unit_test_bags/virginia.edu.uva-lib_2278801.tar")
	require.Nil(t, err)
	upload.Send(file)
	assert.Equal(t, "", upload.ErrorMessage)
}
