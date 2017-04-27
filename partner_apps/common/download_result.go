package common

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"strings"
	"time"
)

type DownloadResult struct {
	Region                 string             `json:"region"`
	Bucket                 string             `json:"bucket"`
	Key                    string             `json:"key"`
	SavedTo                string             `json:"saved_to"`
	Md5                    string             `json:"md5"`
	Sha256                 string             `json:"sha256"`
	BytesDownloaded        int64              `json:"bytes_downloaded"`
	S3ContentLength        int64              `json:"s3_content_length"`
	S3ETag                 string             `json:"s3_etag"`
	S3LastModified         time.Time          `json:"s3_last_modified"`
	S3Metadata             map[string]*string `json:"s3_metadata,omitempty"`
	S3PartsCount           int64              `json:"s3_parts_count,omitempty"`
	S3ServerSideEncryption string             `json:"s3_server_side_encryption,omitempty"`
	S3StorageClass         string             `json:"s3_storage_class,omitempty"`
	S3VersionId            string             `json:"s3_version_id,omitempty"`
	ErrorMessage           string             `json:"error_message,omitempty"`
}

func NewDownloadResult(opts *Options, client *network.S3Download) *DownloadResult {
	result := &DownloadResult{
		Region:          opts.Region,
		Bucket:          opts.Bucket,
		Key:             opts.Key,
		SavedTo:         client.LocalPath,
		Md5:             client.Md5Digest,
		Sha256:          client.Sha256Digest,
		BytesDownloaded: client.BytesCopied,
		ErrorMessage:    client.ErrorMessage,
	}
	if client.Response != nil {
		var contentLength int64
		var lastModified time.Time
		var partsCount int64
		if client.Response.ContentLength != nil {
			contentLength = *client.Response.ContentLength
		}
		if client.Response.LastModified != nil {
			lastModified = *client.Response.LastModified
		}
		if client.Response.PartsCount != nil {
			partsCount = *client.Response.PartsCount
		}
		if client.Response != nil {
			etag := strings.Replace(util.PointerToString(client.Response.ETag), "\"", "", -1)
			result.S3ContentLength = contentLength
			result.S3ETag = etag
			result.S3LastModified = lastModified
			result.S3Metadata = client.Response.Metadata
			result.S3PartsCount = partsCount
			result.S3ServerSideEncryption = util.PointerToString(client.Response.ServerSideEncryption)
			result.S3StorageClass = util.PointerToString(client.Response.StorageClass)
			result.S3VersionId = util.PointerToString(client.Response.VersionId)
		}
	}
	return result
}

// ToJson returns a JSON representation of the result.
// This contains more information than the plain text
// version returned by ToText().
func (result *DownloadResult) ToJson() (string, error) {
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), err
}

// ToText() returns a plain-text representation of the result, suitable
// for printing to STDOUT. To get more detailed information, use ToJson().
func (result *DownloadResult) ToText() string {
	var msg string
	if result.ErrorMessage != "" {
		msg = fmt.Sprintf("[ERROR] Failed to download '%s': %s",
			result.Key, result.ErrorMessage)
	} else {
		msg = fmt.Sprintf("[OK] Downloaded '%s' to '%s'. md5: %s, sha256: %s",
			result.Key, result.SavedTo, result.Md5, result.Sha256)
	}
	return msg
}
