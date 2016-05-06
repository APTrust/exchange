package network_test

import (
	//"fmt"
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

const testBucket string = "aptrust.receiving.test.test.edu"
const testFile string = "virginia.edu.uva-lib_2278801.tar"
const testFileSize int64 = 22528
const testFileETag string = "\"da04d76f9dca11455297827426db35f2\""
const testFileMd5 string = "da04d76f9dca11455297827426db35f2"
const testFileSha256 string = "b917f05da9d9d513160a1aeb51a66065a64b8a51a741ef249060d562926a2365"

// ---------------------------------------------
// TODO: Allow for offline testing!
// ---------------------------------------------

func getS3DownloadObject(t *testing.T) (*network.S3Download) {
	tmpDir, err := ioutil.TempDir("", "s3_download_test")
	if err != nil {
		t.Errorf(err.Error())
		return nil
	}
	tmpFilePath := filepath.Join(tmpDir, testFile)
	return network.NewS3Download(
		constants.AWSVirginia,
		testBucket,
		testFile,
		tmpFilePath,
		false,
		false,
	)
}

func TestGetSession(t *testing.T) {
	download := getS3DownloadObject(t)
	if download == nil {
		return
	}
	session := download.GetSession()
	assert.NotNil(t, session)
	assert.Empty(t, download.ErrorMessage)
}

func TestFetchWithoutChecksums(t *testing.T) {
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
