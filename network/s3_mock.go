package network

import (
	//"encoding/json"
	"fmt"
	"net/http"
	// "net/http/httptest"
	//"net/url"
	//"os"
	//"strings"
)

func getBasicHeaders() map[string]string {
	return map[string]string{
		"x-amz-id-2":       "ef8yU9AS1ed4OpIszj7UDNEHGran",
		"x-amz-request-id": "318BC8BC143432E5",
		"x-amz-version-id": "3HL4kqtJlcpXroDTDmjVBH40Nrjfkd",
		"Date":             "Wed, 30 May 2018 22:32:00 GMT",
		"Last-Modified":    "Tue, 29 May 2018 12:00:00 GMT",
		"ETag":             `"fba9dede5f27731c9771645a39863328"`,
		"Content-Length":   "434234",
		"Content-Type":     "text/plain",
		"Connection":       "close",
		"Server":           "AmazonS3",
	}
}

func S3HeadHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getBasicHeaders() {
		w.Header().Set(key, value)
	}
	fmt.Fprintln(w, "")
}

// Handles a request to restore a Glacier object to S3.
func S3RestoreHandler(w http.ResponseWriter, r *http.Request) {

}

// Handles a request to restore a Glacier object to S3,
// and replies that the restore is already in progress.
func S3RestoreInProgressHandler(w http.ResponseWriter, r *http.Request) {

}

// Handles a request to restore a Glacier object to S3,
// and replies that the item has already been moved
// from Glacier to S3.
func S3RestoreCompletedHandler(w http.ResponseWriter, r *http.Request) {

}
