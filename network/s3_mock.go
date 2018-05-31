package network

import (
	"fmt"
	"net/http"
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

func getRestoreHeaders() map[string]string {
	return map[string]string{
		"x-amz-id-2":                "GFihv3y6+kE7KG11GEkQhU7=",
		"x-amz-request-id":          "9F341CD3C4BA79E0",
		"Date":                      "Wed, 30 May 2018 22:32:00 GMT",
		"Content-Length":            "0",
		"Server":                    "AmazonS3",
		"x-amz-request-charged":     "false",
		"x-amz-restore-output-path": "js-test-s3/qE8nk5M0XIj-LuZE2HXNw6empQm3znLkHlMWInRYPS-Orl2W0uj6LyYm-neTvm1-btz3wbBxfMhPykd3jkl-lvZE7w42/",
	}
}

// Handles a request to restore a Glacier object to S3.
func S3RestoreHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getRestoreHeaders() {
		w.Header().Set(key, value)
	}
	// Must return 202 to indicate the request was accepted
	w.WriteHeader(http.StatusAccepted)
}

// Handles a request to restore a Glacier object to S3,
// and replies with a 409 to say the restoration is
// already in progress.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOSTrestore.html
func S3RestoreInProgressHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getRestoreHeaders() {
		w.Header().Set(key, value)
	}
	// Must return 409 to indicate the request conflicts
	// with one already in progress
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusConflict)
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>RestoreAlreadyInProgress</Code>
  <Message>Object restore is already in progress.</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>`
	fmt.Fprintln(w, xml)
}

// Handles a request to restore a Glacier object to S3,
// and replies with a 200 to say the restoration is
// already completed (item already restored to active tier)
func S3RestoreCompletedHandler(w http.ResponseWriter, r *http.Request) {
	for key, value := range getRestoreHeaders() {
		w.Header().Set(key, value)
	}
	// Must return 200 to indicate item has already
	// been restored to S3
	w.WriteHeader(http.StatusOK)
}
