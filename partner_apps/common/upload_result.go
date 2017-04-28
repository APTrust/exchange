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
	CopiedFrom      string    `json:"copied_from"`
	S3Location      string    `json:"s3_location,omitempty"`
	S3UploadId      string    `json:"s3_upload_id,omitempty"`
	S3LastModified  time.Time `json:"s3_last_modified,omitempty"`
	S3ContentLength int64     `json:"s3_content_length,omitempty"`
	S3ContentType   string    `json:"s3_content_type"`
	S3ETag          string    `json:"s3_etag,omitempty"`
	S3PartsCount    int64     `json:"s3_parts_count,omitempty"`
	ErrorMessage    string    `json:"error_message,omitempty"`
}

func NewUploadResult(opts *Options, uploadClient *network.S3Upload, headClient *network.S3Head) *UploadResult {
	result := &UploadResult{
		Region:       opts.Region,
		Bucket:       opts.Bucket,
		Key:          opts.Key,
		CopiedFrom:   opts.FileToUpload,
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
		etag := strings.Replace(util.PointerToString(headClient.Response.ETag), "\"", "", -1)
		result.S3ContentLength = contentLength
		result.S3ETag = etag
		result.S3PartsCount = partsCount
		result.S3LastModified = lastModified
		result.S3ContentType = util.PointerToString(headClient.Response.ContentType)
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
			result.CopiedFrom, result.ErrorMessage)
	} else {
		msg = fmt.Sprintf("[OK] Uploaded '%s' to '%s/%s' (%d bytes) with etag '%s'",
			result.CopiedFrom, result.Bucket, result.Key,
			result.S3ContentLength, result.S3ETag)
	}
	return msg
}
