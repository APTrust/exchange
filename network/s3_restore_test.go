package network_test

import (
	// "fmt"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
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
	return network.NewS3Restore(
		accessKeyId,
		secretKey,
		"us-east-1",
		"", // bucket must be empty for tests
		"my-file.txt",
		"Standard",
		3)
}

func TestS3RestoreNormal(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = testServer.URL
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	require.NotNil(t, restoreClient.Response)
}

func TestS3RestoreInProgress(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreInProgressHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = testServer.URL
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.True(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	require.NotNil(t, restoreClient.Response)
}

func TestS3RestoreCompleted(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreCompletedHandler))
	defer testServer.Close()
	restoreClient := getS3RestoreClient()
	restoreClient.TestURL = testServer.URL
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)

	// The following is what we want to test, but we
	// can't test it because s3.RestoreObjectOutput
	// gives us no access to the underlying HTTP response code.
	// assert.True(t, restoreClient.AlreadyInActiveTier)
	require.NotNil(t, restoreClient.Response)
}
