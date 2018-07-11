package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestS3Copy(t *testing.T) {
	if !canTestS3() {
		return
	}
	tempName := getS3CopyTempName()
	copier := network.NewS3Copy(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket, // defined in s3_download_test
		testFile,   // defined in s3_download_test
		"aptrust.restore.test.test.edu",
		tempName,
	)
	expectedCopySource := testBucket + "/" + testFile
	assert.Equal(t, expectedCopySource, copier.CopySource())
	copier.Copy()
	assert.NotNil(t, copier.Response)
	require.Empty(t, copier.ErrorMessage)

	// If we did successfully copy an object, delete it.
	deleteCopy(t, tempName)
}

func deleteCopy(t *testing.T, key string) {
	require.NotEmpty(t, key)
	s3ObjectDelete := network.NewS3ObjectDelete(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		"aptrust.restore.test.test.edu",
		[]string{key},
	)
	s3ObjectDelete.DeleteList()
	assert.Equal(t, "", s3ObjectDelete.ErrorMessage)
	assert.Equal(t, 1, len(s3ObjectDelete.Response.Deleted))
	assert.Empty(t, s3ObjectDelete.Response.Errors)
}

func getS3CopyTempName() string {
	return fmt.Sprintf("DELETE_ME_%s", time.Now().UTC().Format(time.RFC3339Nano))
}
