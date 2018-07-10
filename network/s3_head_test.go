package network_test

import (
	"github.com/APTrust/exchange/network"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHead(t *testing.T) {
	// canTestS3, testBucket, testFile, testFileSize,
	// and testFileEtag are defined in s3_download_test
	if !canTestS3() {
		return
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	client := network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustS3Region, testBucket)
	client.Head(testFile) // "virginia.edu.uva-lib_2278801.tar"
	assert.EqualValues(t, testFileSize, *client.Response.ContentLength)
	assert.Equal(t, testFileETag, *client.Response.ETag)
	assert.Equal(t, "application/x-tar", *client.Response.ContentType)
	trimmedETag := strings.Replace(testFileETag, "\"", "", -1)

	storedFile := client.StoredFile()
	assert.NotNil(t, storedFile)
	assert.Equal(t, trimmedETag, storedFile.ETag)

	dpnStoredFile := client.StoredFile()
	assert.NotNil(t, dpnStoredFile)

}

func testGetRestoreInfo(t *testing.T, client *network.S3Head) {
	//client.Response.Restore = `ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"`
	restoreRequestInfo, err := client.GetRestoreRequestInfo()
	assert.Nil(t, err)
	require.NotNil(t, restoreRequestInfo)
	assert.False(t, restoreRequestInfo.RequestInProgress)
	assert.False(t, restoreRequestInfo.RequestIsComplete)
	assert.True(t, restoreRequestInfo.S3ExpiryDate.IsZero())

	ongoing := `ongoing-request="true"`
	client.Response.Restore = &ongoing
	restoreRequestInfo, err = client.GetRestoreRequestInfo()
	assert.Nil(t, err)
	require.NotNil(t, restoreRequestInfo)
	assert.True(t, restoreRequestInfo.RequestInProgress)
	assert.False(t, restoreRequestInfo.RequestIsComplete)
	assert.True(t, restoreRequestInfo.S3ExpiryDate.IsZero())

	completed := `ongoing-request="false", expiry-date="Fri, 23 Dec 2012 00:00:00 GMT"`
	expiryDate, _ := time.Parse(time.RFC1123, "Fri, 23 Dec 2012 00:00:00 GMT")
	client.Response.Restore = &completed
	restoreRequestInfo, err = client.GetRestoreRequestInfo()
	assert.Nil(t, err)
	require.NotNil(t, restoreRequestInfo)
	assert.False(t, restoreRequestInfo.RequestInProgress)
	assert.True(t, restoreRequestInfo.RequestIsComplete)
	assert.Equal(t, expiryDate, restoreRequestInfo.S3ExpiryDate)
}
