package network_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewS3ObjectList(t *testing.T) {
	if !canTestS3() {
		return
	}
	s3ObjectList := network.NewS3ObjectList(
		constants.AWSVirginia,
		testBucket,
		int64(100),
	)
	assert.Equal(t, testBucket, *s3ObjectList.ListObjectsInput.Bucket)
	assert.Equal(t, constants.AWSVirginia, s3ObjectList.AWSRegion)
	assert.Equal(t, int64(100), *s3ObjectList.ListObjectsInput.MaxKeys)
}
