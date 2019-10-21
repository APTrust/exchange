package network_test

import (
	// "fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func getS3RestoreClient() *network.S3Restore {
	accessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKeyId == "" {
		accessKeyId = "int_test_access_key_id"
	}
	if secretKey == "" {
		secretKey = "int_test_secret_key"
	}

	// A.D.: 2019-10-03
	//
	// HACK: The AWS S3 client prepends the bucket name to the
	// S3 URL before it makes a request. Our mock S3 service
	// runs on 127.0.0.1. We have to remove the "127." prefix on
	// our mock service URL in the tests below so that when
	// the AWS S3 client prepends it, it resolves to the correct
	// IP address.
	return network.NewS3Restore(
		accessKeyId,
		secretKey,
		"us-east-1",
		constants.AWS_TEST_HACK_BUCKET_NAME,
		"my-file.txt",
		"Standard",
		3)
}

func TestS3RestoreNormal(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = strings.Replace(testServer.URL, constants.AWS_TEST_HACK_IP_PREFIX, "", 1)
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	assert.False(t, restoreClient.RequestRejectedServiceUnavailable)
	assert.True(t, restoreClient.RequestAccepted())
	require.NotNil(t, restoreClient.Response)
}

func TestS3RestoreInProgress(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreInProgressHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = strings.Replace(testServer.URL, constants.AWS_TEST_HACK_IP_PREFIX, "", 1)
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.True(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	assert.False(t, restoreClient.RequestRejectedServiceUnavailable)
	assert.True(t, restoreClient.RequestAccepted())
	require.NotNil(t, restoreClient.Response)
}

func TestS3RestoreCompleted(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreCompletedHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = strings.Replace(testServer.URL, constants.AWS_TEST_HACK_IP_PREFIX, "", 1)
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.RequestRejectedServiceUnavailable)
	assert.True(t, restoreClient.RequestAccepted())

	// The following is what we want to test, but we
	// can't test it because s3.RestoreObjectOutput
	// gives us no access to the underlying HTTP response code.
	// assert.True(t, restoreClient.AlreadyInActiveTier)
	require.NotNil(t, restoreClient.Response)
}

func TestS3RestoreRejected(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreRejectHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = strings.Replace(testServer.URL, constants.AWS_TEST_HACK_IP_PREFIX, "", 1)
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	assert.True(t, restoreClient.RequestRejectedServiceUnavailable)
	assert.False(t, restoreClient.RequestAccepted())
	require.NotNil(t, restoreClient.Response)
}
