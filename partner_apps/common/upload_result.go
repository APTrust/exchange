package common

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"strings"
	"time"
)

type UploadResult struct {
	Region          string    `json:"region"`
	Bucket          string    `json:"bucket"`
	Key             string    `json:"key"`
	S3Location      string    `json:"s3_location"`
	S3UploadId      string    `json:"s3_upload_id"`
	S3LastModified  time.Time `json:"s3_last_modified"`
	S3ContentLength int64     `json:"s3_content_length"`
	S3ETag          string    `json:"s3_etag"`
	S3PartsCount    int64     `json:"s3_parts_count,omitempty"`
	ErrorMessage    string    `json:"error_message,omitempty"`
}

func NewUploadResult(opts *Options, uploadClient *network.S3Upload, headClient *network.S3Head) *UploadResult {
	result := &UploadResult{
		Region:       opts.Region,
		Bucket:       opts.Bucket,
		Key:          opts.Key,
		ErrorMessage: uploadClient.ErrorMessage,
	}
	if uploadClient.Response != nil {
		result.S3Location = uploadClient.Response.Location
		result.S3UploadId = uploadClient.Response.UploadID
	}
	if headClient.Response != nil {
		var contentLength int64
		var partsCount int64
		var lastModified time.Time
		if headClient.Response.ContentLength != nil {
			contentLength = *headClient.Response.ContentLength
		}
		if headClient.Response.LastModified != nil {
			lastModified = *headClient.Response.LastModified
		}
		if headClient.Response.PartsCount != nil {
			partsCount = *headClient.Response.PartsCount
		}
		if headClient.Response != nil {
			etag := strings.Replace(util.PointerToString(headClient.Response.ETag), "\"", "", -1)
			result.S3ContentLength = contentLength
			result.S3ETag = etag
			result.S3PartsCount = partsCount
			result.S3LastModified = lastModified
		}
		if headClient.ErrorMessage != "" {
			result.ErrorMessage += headClient.ErrorMessage
		}
	}
	return result
}

// ToJson returns a JSON representation of the result.
// This contains more information than the plain text
// version returned by ToText().
func (result *UploadResult) ToJson() (string, error) {
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), err
}

// ToText() returns a plain-text representation of the result, suitable
// for printing to STDOUT. To get more detailed information, use ToJson().
func (result *UploadResult) ToText() string {
	var msg string
	if result.ErrorMessage != "" {
		msg = fmt.Sprintf("[ERROR] Failed to upload '%s': %s",
			result.Key, result.ErrorMessage)
	} else {
		msg = fmt.Sprintf("[OK] Uploaded '%s' to '%s' with etag '%s'",
			result.Key, result.Bucket, result.S3ETag)
	}
	return msg
}
