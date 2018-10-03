package network_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewS3ObjectList(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	s3ObjectList := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		int64(100),
	)
	assert.Equal(t, testBucket, *s3ObjectList.ListObjectsInput.Bucket)
	assert.Equal(t, constants.AWSVirginia, s3ObjectList.AWSRegion)
	assert.Equal(t, int64(100), *s3ObjectList.ListObjectsInput.MaxKeys)
}

func TestS3ObjectGetList(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	s3ObjectList := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		int64(100),
	)
	s3ObjectList.GetList("")
	assert.Equal(t, "", s3ObjectList.ErrorMessage)
	assert.NotEmpty(t, s3ObjectList.Response.Contents)
}
