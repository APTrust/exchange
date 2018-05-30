package network_test

import (
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestS3HeadHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3HeadHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	testGeneralHeaders(t, resp)
}

func TestS3HeadRestoreInProgressHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3HeadRestoreInProgressHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	testGeneralHeaders(t, resp)
	assert.Equal(t, `ongoing-request="true"`, resp.Header.Get("x-amz-restore"))
}

func TestS3HeadRestoreCompletedHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3HeadRestoreCompletedHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	testGeneralHeaders(t, resp)
	assert.Equal(t, `ongoing-request="false", expiry-date="Fri, 1 Jun 2018 04:00:00 GMT"`, resp.Header.Get("x-amz-restore"))
}

func testGeneralHeaders(t *testing.T, resp *http.Response) {
	assert.Equal(t, "ef8yU9AS1ed4OpIszj7UDNEHGran", resp.Header.Get("x-amz-id-2"))
	assert.Equal(t, "318BC8BC143432E5", resp.Header.Get("x-amz-request-id"))
	assert.Equal(t, "3HL4kqtJlcpXroDTDmjVBH40Nrjfkd", resp.Header.Get("x-amz-version-id"))
	assert.Equal(t, "Wed, 30 May 2018 22:32:00 GMT", resp.Header.Get("Date"))
	assert.Equal(t, "Tue, 29 May 2018 12:00:00 GMT", resp.Header.Get("Last-Modified"))
	assert.Equal(t, `"fba9dede5f27731c9771645a39863328"`, resp.Header.Get("Etag"))
	assert.Equal(t, "434234", resp.Header.Get("Content-Length"))
	assert.Equal(t, "text/plain", resp.Header.Get("Content-Type"))
	assert.Equal(t, "AmazonS3", resp.Header.Get("Server"))
}

func TestS3RestoreHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	testGeneralRestoreHeaders(t, resp)
}

func TestS3RestoreInProgressHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreInProgressHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	testGeneralRestoreHeaders(t, resp)
}

func TestS3RestoreCompletedHandler(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(network.S3RestoreCompletedHandler))
	defer testServer.Close()
	resp, err := http.Head(testServer.URL)
	require.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	testGeneralRestoreHeaders(t, resp)
}

func testGeneralRestoreHeaders(t *testing.T, resp *http.Response) {
	assert.Equal(t, "GFihv3y6+kE7KG11GEkQhU7=", resp.Header.Get("x-amz-id-2"))
	assert.Equal(t, "9F341CD3C4BA79E0", resp.Header.Get("x-amz-request-id"))
	assert.Equal(t, "Wed, 30 May 2018 22:32:00 GMT", resp.Header.Get("Date"))
	assert.Equal(t, "0", resp.Header.Get("Content-Length"))
	assert.Equal(t, "AmazonS3", resp.Header.Get("Server"))
}
