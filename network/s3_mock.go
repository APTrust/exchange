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

// Handles an S3 Head request and replies that a restore is already in progress.
func S3HeadRestoreInProgressHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getBasicHeaders() {
		w.Header().Set(key, value)
	}
	w.Header().Set("x-amz-restore", `ongoing-request="true"`)
	fmt.Fprintln(w, "")
}

// Handles an S3 Head request and replies that a restore is complete.
func S3HeadRestoreCompletedHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getBasicHeaders() {
		w.Header().Set(key, value)
	}
	w.Header().Set("x-amz-restore", `ongoing-request="false", expiry-date="Fri, 1 Jun 2018 04:00:00 GMT"`)
	fmt.Fprintln(w, "")
}

// Handles a request to restore a Glacier object to S3.
func S3RestoreHandler(w http.ResponseWriter, r *http.Request) {

}
