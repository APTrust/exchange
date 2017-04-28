package common_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func getUploadClient() (*network.S3Upload, error) {
	relpath := filepath.Join("testdata", "json_objects", "s3_upload_output.json")
	filename, err := fileutil.RelativeToAbsPath(relpath)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	var uploadOutput *s3manager.UploadOutput
	err = json.Unmarshal(bytes, &uploadOutput)
	if err != nil {
		return nil, fmt.Errorf("Error parsing JSON from file '%s': %v", filename, err)
	}
	client := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		"test.bucket",
		"TestKey",
		"text/plain")
	client.Response = uploadOutput
	return client, nil
}

func getHeadClient() (*network.S3Head, error) {
	relpath := filepath.Join("testdata", "json_objects", "s3_head_object_output.json")
	filename, err := fileutil.RelativeToAbsPath(relpath)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	var headOutput *s3.HeadObjectOutput
	err = json.Unmarshal(bytes, &headOutput)
	if err != nil {
		return nil, fmt.Errorf("Error parsing JSON from file '%s': %v", filename, err)
	}
	client := network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		"test.bucket")
	client.Response = headOutput
	return client, nil
}

func TestNewUploadResult(t *testing.T) {
	// Note that getOpts is defined in options_test.go
	opts := getOpts()
	uploadClient, err := getUploadClient()
	require.Nil(t, err)
	headClient, err := getHeadClient()
	require.Nil(t, err)
	result := common.NewUploadResult(opts, uploadClient, headClient)
	require.NotNil(t, result)
	assert.Equal(t, opts.Region, result.Region)
	assert.Equal(t, opts.Bucket, result.Bucket)
	assert.Equal(t, opts.Key, result.Key)
	assert.Equal(t, opts.FileToUpload, result.CopiedFrom)
	assert.Equal(t, "", result.ErrorMessage)
	assert.Equal(t, int64(23552), result.S3ContentLength)
	assert.Equal(t, "05e68e69767c772d36bd8a2baf693428", result.S3ETag)
	lastModified, err := time.Parse(time.RFC3339, "2017-04-28T19:25:27Z")
	assert.Nil(t, err)
	assert.Equal(t, lastModified, result.S3LastModified)
	assert.Equal(t, int64(0), result.S3PartsCount)
	assert.Equal(t, "application/tar", result.S3ContentType)
}

func TestUploadResultToJson(t *testing.T) {
	opts := getOpts()
	uploadClient, err := getUploadClient()
	require.Nil(t, err)
	headClient, err := getHeadClient()
	require.Nil(t, err)
	result := common.NewUploadResult(opts, uploadClient, headClient)
	require.NotNil(t, result)
	jsonString, err := result.ToJson()
	require.Nil(t, err)
	expected := `{"region":"us-east-1","bucket":"test.bucket","key":"TestKey","copied_from":"/dev/null/file.txt","s3_location":"https://s3.amazonaws.com/aptrust.test/MyTestFile2.tar","s3_upload_id":"54321","s3_last_modified":"2017-04-28T19:25:27Z","s3_content_length":23552,"s3_content_type":"application/tar","s3_etag":"05e68e69767c772d36bd8a2baf693428"}`
	assert.Equal(t, expected, jsonString)
}

func TestUploadResultToText(t *testing.T) {
	opts := getOpts()
	uploadClient, err := getUploadClient()
	require.Nil(t, err)
	headClient, err := getHeadClient()
	require.Nil(t, err)
	result := common.NewUploadResult(opts, uploadClient, headClient)
	require.NotNil(t, result)
	assert.Equal(t, "[OK] Uploaded '/dev/null/file.txt' to 'test.bucket/TestKey' (23552 bytes) with etag '05e68e69767c772d36bd8a2baf693428'", result.ToText())
}
