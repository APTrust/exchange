package network_test

import (
	"github.com/APTrust/exchange/network"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHead(t *testing.T) {
	// canTestS3, testBucket, testFile, testFileSize,
	// and testFileEtag are defined in s3_download_test
	if !canTestS3() {
		return
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	client := network.NewS3Head(_context.Config.APTrustS3Region, testBucket)
	client.Head(testFile)
	require.Empty(t, client.ErrorMessage)
	assert.EqualValues(t, testFileSize, *client.Response.ContentLength)
	assert.Equal(t, testFileETag, *client.Response.ETag)
	assert.Equal(t, "application/x-tar", *client.Response.ContentType)
}
