package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// A copy of the test file, virginia.edu.uva-lib_2278801.tar,
// is in the testdata directory so you can put it back on S3
// if it ever gets deleted.

const testBucket string = "aptrust.integration.test"
const testFile string = "virginia.edu.uva-lib_2278801.tar"
const testFileSize int64 = 30720
const testFileETag string = "\"036995504a5b07a865b62e1a7c0ea9c4\""
const testFileMd5 string = "036995504a5b07a865b62e1a7c0ea9c4"
const testFileSha256 string = "a909bbe46dedfc15918bfa94d0fd86bad9d9e1d2aa2afdedbfc11e7f52582eab"

var s3TestMessagePrinted = false

func canTestS3() bool {
	// Note that, to run S3 and Glacier tests, these vars not only have to be set,
	// they have to be valid keys with read/write access to the buckets specified
	// in the config file.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		if !s3TestMessagePrinted {
			fmt.Println("Skipping S3 tests because ENV vars are not set")
		}
		return false
	}
	return true
}

func getS3DownloadObject(t *testing.T) *network.S3Download {
	tmpDir, err := ioutil.TempDir("", "s3_download_test")
	if err != nil {
		t.Errorf(err.Error())
		return nil
	}
	tmpFilePath := filepath.Join(tmpDir, testFile)
	return network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		testFile,
		tmpFilePath,
		false,
		false,
	)
}

func TestGetSession(t *testing.T) {
	if !canTestS3() {
		return
	}
	download := getS3DownloadObject(t)
	if download == nil {
		return
	}
	session := download.GetSession()
	assert.NotNil(t, session)
	assert.Empty(t, download.ErrorMessage)
}

func TestFetchWithoutChecksums(t *testing.T) {
	if !canTestS3() {
		return
	}
	download := getS3DownloadObject(t)
	if download == nil {
		return
	}
	download.CalculateMd5 = false
	download.CalculateSha256 = false
	download.Fetch()
	defer os.Remove(download.LocalPath)

	assert.Empty(t, download.ErrorMessage)
	if download.Response == nil {
		assert.FailNow(t, "Response object is nil")
	}

	assert.Equal(t, testFileETag, *download.Response.ETag)
	assert.Equal(t, int64(testFileSize), *download.Response.ContentLength)
	assert.Equal(t, int64(testFileSize), download.BytesCopied)

	fileStat, err := os.Stat(download.LocalPath)
	if err != nil {
		assert.FailNow(t, "Download file '%s' does not exist", download.LocalPath)
	}
	assert.Equal(t, int64(testFileSize), fileStat.Size())

	assert.Empty(t, download.Md5Digest)
	assert.Empty(t, download.Sha256Digest)
}

func TestFetchWithChecksums(t *testing.T) {
	if !canTestS3() {
		return
	}
	download := getS3DownloadObject(t)
	if download == nil {
		return
	}
	download.CalculateMd5 = true
	download.CalculateSha256 = true

	download.Fetch()
	defer os.Remove(download.LocalPath)

	assert.Empty(t, download.ErrorMessage)
	if download.Response == nil {
		assert.FailNow(t, "Response object is nil")
	}

	assert.Equal(t, testFileETag, *download.Response.ETag)
	assert.Equal(t, int64(testFileSize), *download.Response.ContentLength)

	assert.Equal(t, int64(testFileSize), download.BytesCopied)

	fileStat, err := os.Stat(download.LocalPath)
	if err != nil {
		assert.FailNow(t, "Download file '%s' does not exist", download.LocalPath)
	}
	assert.Equal(t, int64(testFileSize), fileStat.Size())

	assert.Equal(t, testFileMd5, download.Md5Digest)
	assert.Equal(t, testFileSha256, download.Sha256Digest)
}
