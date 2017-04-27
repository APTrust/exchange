package common_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func getDownloadClient() (*network.S3Download, error) {
	relpath := filepath.Join("testdata", "json_objects", "s3_list_object_output.json")
	filename, err := fileutil.RelativeToAbsPath(relpath)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	var s3GetObjectOutput s3.GetObjectOutput
	err = json.Unmarshal(bytes, &s3GetObjectOutput)
	if err != nil {
		return nil, fmt.Errorf("Error parsing JSON from file '%s': %v", filename, err)
	}
	client := network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		"test.bucket",
		"TestKey",
		"~/tmp/",
		true,
		true,
	)
	client.Response = &s3GetObjectOutput
	client.Md5Digest = "12345"
	client.Sha256Digest = "54321"
	return client, nil
}

func getOpts() *common.Options {
	return &common.Options{
		PathToConfigFile: "",
		AccessKeyId:      "Ax S Kee Eye Dee",
		AccessKeyFrom:    "environment",
		SecretAccessKey:  "Shh! Seekrit!",
		SecretKeyFrom:    "environment",
		Region:           "us-east-1",
		Bucket:           "test.bucket",
		Key:              "TestKey",
		Dir:              "tmp/",
		OutputFormat:     "json",
	}
}

func TestNewDownloadResut(t *testing.T) {
	opts := getOpts()
	client, err := getDownloadClient()
	require.Nil(t, err)
	result := common.NewDownloadResult(opts, client)
	require.NotNil(t, result)
	assert.Equal(t, opts.Region, result.Region)
	assert.Equal(t, opts.Bucket, result.Bucket)
	assert.Equal(t, opts.Key, result.Key)
	assert.Equal(t, "~/tmp/", result.SavedTo)
	assert.Equal(t, "12345", result.Md5)
	assert.Equal(t, "54321", result.Sha256)
	assert.Equal(t, int64(0), result.BytesDownloaded)
	assert.Equal(t, "", result.ErrorMessage)
	assert.Equal(t, int64(1635), result.S3ContentLength)
	assert.Equal(t, "e42935a09f6cb31646a814e321ea8fa0", result.S3ETag)
	lastModified, err := time.Parse(time.RFC3339, "2017-01-22T19:10:55Z")
	assert.Nil(t, err)
	assert.Equal(t, lastModified, result.S3LastModified)
	assert.NotEmpty(t, result.S3Metadata)
	assert.Equal(t, int64(0), result.S3PartsCount)
	assert.Equal(t, "", result.S3ServerSideEncryption)
	assert.Equal(t, "", result.S3StorageClass)
	assert.Equal(t, "", result.S3VersionId)
}

func TestToJson(t *testing.T) {

}

func TestToText(t *testing.T) {

}
