package common_test

import (
	"github.com/APTrust/exchange/partner_apps/common"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/partner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		FileToUpload:     "/dev/null/file.txt",
		OutputFormat:     "json",
	}
}

func TestSetAndVerifyDownloadOptions(t *testing.T) {
	keyId := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	defer os.Setenv("AWS_ACCESS_KEY_ID", keyId)
	defer os.Setenv("AWS_SECRET_ACCESS_KEY", secret)

	opts := common.Options{}
	opts.PathToConfigFile = "/dev/null"
	opts.SetAndVerifyDownloadOptions()
	assert.Equal(t, 4, len(opts.Errors()))

	opts = common.Options{}
	filePath, err := getConfigFilePath()
	require.Nil(t, err)
	opts.PathToConfigFile = filePath
	opts.SetAndVerifyDownloadOptions()
	assert.Equal(t, 1, len(opts.Errors()), opts.Errors)

	// Should NOT overwrite opts if explicitly set.
	conf := getTestConfig(t)
	require.NotNil(t, conf)
	opts = common.Options{
		AccessKeyId:      "1234",
		SecretAccessKey:  "5678",
		PathToConfigFile: filePath,
		Key:              "key",
		Bucket:           "bucket",
		Dir:              "dir",
	}
	opts.SetAndVerifyDownloadOptions()
	assert.Equal(t, "bucket", opts.Bucket)
	assert.Equal(t, "dir", opts.Dir)
	assert.Equal(t, "1234", opts.AccessKeyId)
	assert.Equal(t, "5678", opts.SecretAccessKey)
	assert.Equal(t, "bucket", opts.Bucket)
	assert.Empty(t, opts.Errors())

	// Should set opts from file if not explicitly set.
	opts = common.Options{
		PathToConfigFile: filePath,
		Key:              "key",
	}
	opts.SetAndVerifyDownloadOptions()
	assert.Equal(t, conf.RestorationBucket, opts.Bucket)
	assert.Equal(t, conf.DownloadDir, opts.Dir)
	assert.Equal(t, conf.AwsAccessKeyId, opts.AccessKeyId)
	assert.Equal(t, conf.AwsSecretAccessKey, opts.SecretAccessKey)
	assert.Empty(t, opts.Errors())
}

func TestSetAndVerifyUploadOptions(t *testing.T) {
	keyId := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	defer os.Setenv("AWS_ACCESS_KEY_ID", keyId)
	defer os.Setenv("AWS_SECRET_ACCESS_KEY", secret)

	opts := common.Options{}
	opts.PathToConfigFile = "/dev/null"
	opts.SetAndVerifyUploadOptions()
	assert.Equal(t, 4, len(opts.Errors()))

	opts = common.Options{}
	filePath, err := getConfigFilePath()
	require.Nil(t, err)
	opts.PathToConfigFile = filePath
	opts.SetAndVerifyUploadOptions()
	assert.Equal(t, 1, len(opts.Errors()), opts.Errors)

	// Should NOT overwrite opts if explicitly set.
	conf := getTestConfig(t)
	require.NotNil(t, conf)
	opts = common.Options{
		AccessKeyId:      "1234",
		SecretAccessKey:  "5678",
		PathToConfigFile: filePath,
		Bucket:           "bucket",
		FileToUpload:     "./some/file.txt",
	}
	opts.SetAndVerifyUploadOptions()
	assert.Equal(t, "bucket", opts.Bucket)
	assert.Equal(t, "1234", opts.AccessKeyId)
	assert.Equal(t, "5678", opts.SecretAccessKey)
	assert.Equal(t, "bucket", opts.Bucket)
	assert.Equal(t, "./some/file.txt", opts.FileToUpload)
	assert.Empty(t, opts.Errors())

	// Should set opts from file if not explicitly set.
	opts = common.Options{
		PathToConfigFile: filePath,
		FileToUpload:     "./some/file.txt",
	}
	opts.SetAndVerifyUploadOptions()
	assert.Equal(t, conf.RestorationBucket, opts.Bucket)
	assert.Equal(t, conf.DownloadDir, opts.Dir)
	assert.Equal(t, conf.AwsAccessKeyId, opts.AccessKeyId)
	assert.Equal(t, conf.AwsSecretAccessKey, opts.SecretAccessKey)
	assert.Empty(t, opts.Errors())
}

func TestVerifyRequiredDownloadOptions(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredDownloadOptions()
	assert.Equal(t, 4, len(opts.Errors()))

	filePath, err := getConfigFilePath()
	require.Nil(t, err)
	opts.PathToConfigFile = filePath
	opts.ClearErrors()
	opts.MergeConfigFileOptions()
	opts.VerifyRequiredDownloadOptions()
	assert.Equal(t, 1, len(opts.Errors()))

	opts.Key = "key"
	opts.ClearErrors()
	opts.VerifyRequiredDownloadOptions()
	assert.Empty(t, opts.Errors())
}

func TestVerifyRequiredUploadOptions(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredUploadOptions()
	assert.Equal(t, 4, len(opts.Errors()))

	filePath, err := getConfigFilePath()
	require.Nil(t, err)
	opts.PathToConfigFile = filePath
	opts.ClearErrors()
	opts.MergeConfigFileOptions()
	opts.VerifyRequiredUploadOptions()
	assert.Equal(t, 1, len(opts.Errors()))

	opts.FileToUpload = "/home/joy/video.mp4"
	opts.ClearErrors()
	opts.VerifyRequiredUploadOptions()
	assert.Empty(t, opts.Errors())
}

func TestVerifyOutputFormat(t *testing.T) {
	opts := common.Options{}
	opts.OutputFormat = "text"
	opts.VerifyOutputFormat()
	assert.Empty(t, opts.Errors())

	opts.OutputFormat = "json"
	opts.VerifyOutputFormat()
	assert.Empty(t, opts.Errors())

	opts.OutputFormat = "canary"
	opts.VerifyOutputFormat()
	assert.Equal(t, 1, len(opts.Errors()))
}

func TestEnsureDownloadDirIsSet(t *testing.T) {
	opts := common.Options{}
	opts.Dir = ""
	opts.EnsureDownloadDirIsSet()
	expected, _ := os.Getwd()
	assert.Equal(t, expected, opts.Dir)

	opts.Dir = "~/tmp"
	opts.EnsureDownloadDirIsSet()
	assert.True(t, len(opts.Dir) > len("~/tmp") && strings.HasPrefix(opts.Dir, string(os.PathSeparator)))
}

func TestMergeConfigFileOptions(t *testing.T) {
	filePath, err := getConfigFilePath()
	require.Nil(t, err)

	conf := getTestConfig(t)
	require.NotNil(t, conf)

	// Now make sure values are merged correctly.
	// These four options, if not explicitly supplied
	// by the user, should be pulled from the config file.
	opts := &common.Options{
		PathToConfigFile: filePath,
	}
	opts.MergeConfigFileOptions()
	assert.Equal(t, conf.RestorationBucket, opts.Bucket)
	assert.Equal(t, conf.DownloadDir, opts.Dir)
	assert.Equal(t, conf.AwsAccessKeyId, opts.AccessKeyId)
	assert.Equal(t, conf.AwsSecretAccessKey, opts.SecretAccessKey)
}

func TestLoadConfigFile(t *testing.T) {
	opts := &common.Options{}

	if partner.DefaultConfigFileExists() {
		conf, err := opts.LoadConfigFile()
		assert.Nil(t, err)
		assert.NotNil(t, conf)
	}

	conf := getTestConfig(t)
	require.NotNil(t, conf)
}

func TestHasErrors(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredDownloadOptions()
	assert.True(t, opts.HasErrors())
}

func TestErrors(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredDownloadOptions()
	assert.Equal(t, 4, len(opts.Errors()))
}

func TestClearErrors(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredDownloadOptions()
	assert.Equal(t, 4, len(opts.Errors()))
	opts.ClearErrors()
	assert.Empty(t, opts.Errors())
}

func AllErrorsAsString(t *testing.T) {
	opts := common.Options{}
	opts.VerifyRequiredDownloadOptions()
	assert.Equal(t, "", opts.AllErrorsAsString())
}

func getTestConfig(t *testing.T) *common.PartnerConfig {
	filePath, err := getConfigFilePath()
	require.Nil(t, err)
	opts := &common.Options{}
	opts.PathToConfigFile = filePath
	conf, err := opts.LoadConfigFile()
	assert.Nil(t, err)
	assert.NotNil(t, conf)
	return conf
}

func getConfigFilePath() (string, error) {
	f := filepath.Join("testdata", "config", "partner_config_valid.conf")
	return fileutil.RelativeToAbsPath(f)
}
