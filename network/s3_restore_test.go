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
	return network.NewS3Restore(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
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
	//require.Empty(t, restoreClient.ErrorMessage)
	assert.True(t, restoreClient.RestoreAlreadyInProgress, "This is failing because the s3_mock.go is wrong")
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
	assert.True(t, restoreClient.AlreadyInActiveTier, "This is failing because the s3_mock.go is wrong")
	require.NotNil(t, restoreClient.Response)
}
