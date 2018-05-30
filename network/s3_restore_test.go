package network_test

import (
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestS3RestoreNormal(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreHandler))
	defer testServer.Close()
	restoreClient := network.NewS3Restore(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"us-east-1",
		"", // bucket must be empty for tests
		"my-file.txt",
		"Standard",
		3)
	restoreClient.TestURL = testServer.URL
	restoreClient.Restore()
	require.Empty(t, restoreClient.ErrorMessage)
	assert.False(t, restoreClient.RestoreAlreadyInProgress)
	assert.False(t, restoreClient.AlreadyInActiveTier)
	require.NotNil(t, restoreClient.Response)
}

// func TestS3RestoreInProgress(t *testing.T) {
// 	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreInProgressHandler))
// 	defer testServer.Close()
// 	resp, err := http.Head(testServer.URL)
// 	require.Nil(t, err)
// 	assert.Equal(t, http.StatusConflict, resp.StatusCode)
// 	testGeneralRestoreHeaders(t, resp)
// }

// func TestS3RestoreCompleted(t *testing.T) {
// 	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreCompletedHandler))
// 	defer testServer.Close()
// 	resp, err := http.Head(testServer.URL)
// 	require.Nil(t, err)
// 	assert.Equal(t, http.StatusOK, resp.StatusCode)
// 	testGeneralRestoreHeaders(t, resp)
// }
