package common_test

import (
	//	"encoding/json"
	//	"fmt"
	//	"github.com/APTrust/exchange/constants"
	//	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	//	"github.com/APTrust/exchange/util/fileutil"
	//	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	//	"github.com/stretchr/testify/require"
	//	"io/ioutil"
	"os"
	//	"path/filepath"
	"strings"
	"testing"
	//	"time"
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
		OutputFormat:     "json",
	}
}

func TestSetAndVerifyDownloadOptions(t *testing.T) {

}

func TestVerifyRequiredDownloadOptions(t *testing.T) {

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

}

func TestLoadConfigFile(t *testing.T) {

}

func TestHasErrors(t *testing.T) {

}

func TestErrors(t *testing.T) {

}

func AllErrorsAsString(t *testing.T) {

}
