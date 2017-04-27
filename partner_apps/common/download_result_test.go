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
	opts := getOpts()
	client, err := getDownloadClient()
	require.Nil(t, err)
	result := common.NewDownloadResult(opts, client)
	require.NotNil(t, result)
	jsonString, err := result.ToJson()
	require.Nil(t, err)
	expected := `{"region":"us-east-1","bucket":"test.bucket","key":"TestKey","saved_to":"~/tmp/","md5":"12345","sha256":"54321","bytes_downloaded":0,"s3_content_length":1635,"s3_etag":"e42935a09f6cb31646a814e321ea8fa0","s3_last_modified":"2017-01-22T19:10:55Z","s3_metadata":{"Bag":"uc.edu/cin.websites.2016-12-15","Bagpath":"data/www/html/oesper/museum/case20/shelf_01/RF0001/views/RF0001_view2/TileGroup0/4-9-1.jpg","Institution":"uc.edu","Md5":"e42935a09f6cb31646a814e321ea8fa0","Sha256":"41a5a19c45022e715b9743a8cd9fdb3aeb1f7f044ef493b7cfb0e2eae1797820"}}`
	assert.Equal(t, expected, jsonString)
}

func TestToText(t *testing.T) {
	opts := getOpts()
	client, err := getDownloadClient()
	require.Nil(t, err)
	result := common.NewDownloadResult(opts, client)
	require.NotNil(t, result)
	assert.Equal(t, "[OK] Downloaded 'TestKey' to '~/tmp/'. md5: 12345, sha256: 54321", result.ToText())
}
