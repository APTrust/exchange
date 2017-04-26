package models_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadPartnerConfigGood(t *testing.T) {
	filePath, err := fileutil.RelativeToAbsPath(filepath.Join("testdata", "config", "partner_config_valid.conf"))
	require.Nil(t, err)
	partnerConfig, err := models.LoadPartnerConfig(filePath)
	require.Nil(t, err)
	assert.Equal(t, "123456789XYZ", partnerConfig.AwsAccessKeyId)
	assert.Equal(t, "THIS KEY INCLUDES SPACES AND DOES NOT NEED QUOTES", partnerConfig.AwsSecretAccessKey)

	assert.Equal(t, "aptrust.receiving.testbucket.edu", partnerConfig.ReceivingBucket)
	assert.Equal(t, "aptrust.restore.testbucket.edu", partnerConfig.RestorationBucket)
}

func TestLoadPartnerConfigWrongFileType(t *testing.T) {
	filePath, err := fileutil.RelativeToAbsPath(filepath.Join("testdata", "config", "intel_obj.json"))
	require.Nil(t, err)
	_, err = models.LoadPartnerConfig(filePath)
	require.NotNil(t, err)
}

func TestLoadPartnerConfigMissingFile(t *testing.T) {
	filePath, err := fileutil.RelativeToAbsPath(filepath.Join("testdata", "config", "_non_existent_file.conf_"))
	require.Nil(t, err)
	_, err = models.LoadPartnerConfig(filePath)
	require.NotNil(t, err)
}

func TestLoadAwsFromEnv(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		fmt.Println("Skipping AWS env test. Env vars are not set.")
		return
	}
	filePath, err := fileutil.RelativeToAbsPath(filepath.Join("testdata", "config", "partner_config_invalid.conf"))
	require.Nil(t, err)

	partnerConfig, err := models.LoadPartnerConfig(filePath)
	require.Nil(t, err)

	if partnerConfig.AwsAccessKeyId != "" {
		t.Errorf("Test precondition is invalid. AwsAccessKeyId has a value.")
	}
	if partnerConfig.AwsSecretAccessKey != "" {
		t.Errorf("Test precondition is invalid. AwsSecretAccessKey has a value.")
	}
	partnerConfig.LoadAwsFromEnv()
	assert.NotEmpty(t, partnerConfig.AwsAccessKeyId)
	assert.NotEmpty(t, partnerConfig.AwsSecretAccessKey)
}

func TestLoadPartnerConfigBad(t *testing.T) {
	filePath, err := fileutil.RelativeToAbsPath(filepath.Join("testdata", "config", "partner_config_invalid.conf"))
	require.Nil(t, err)
	partnerConfig, err := models.LoadPartnerConfig(filePath)
	require.Nil(t, err)
	// Make sure we get warnings on unexpected settings and on
	// expected settings that are not there.
	warnings := partnerConfig.Warnings()
	assert.Equal(t, 7, len(warnings))
	assert.Equal(t, "Invalid setting: FavoriteTeam = The home team", warnings[0])
	assert.Equal(t, "Invalid setting: FavoriteFlavor = Green", warnings[1])
	assert.True(t, strings.HasPrefix(warnings[2], "AwsAccessKeyId"),
		"Did not get expected warning about invalid setting")
	assert.True(t, strings.HasPrefix(warnings[2], "AwsAccessKeyId"),
		"Did not get expected warning about missing AwsAccessKeyId")
	assert.True(t, strings.HasPrefix(warnings[3], "AwsSecretAccessKey is missing"),
		"Did not get expected warning about missing AwsSecretAccessKey")
	assert.True(t, strings.HasPrefix(warnings[4], "ReceivingBucket is missing"),
		"Did not get expected warning about missing ReceivingBucket")
	assert.True(t, strings.HasPrefix(warnings[5], "RestorationBucket is missing"),
		"Did not get expected warning about missing RestorationBucket")
	assert.True(t, strings.HasPrefix(warnings[6], "DownloadDir is missing"),
		"Did not get expected warning about missing DownloadDir")
}

func TestPartnerConfigValidate(t *testing.T) {
	partnerConfig := &models.PartnerConfig{
		AwsAccessKeyId:     "abc",
		AwsSecretAccessKey: "xyz",
		ReceivingBucket:    "aptrust.receiving.xyz.edu",
		RestorationBucket:  "aptrust.receiving.xyz.edu",
		DownloadDir:        "/home/josie/tmp",
	}

	// Clear these out for this test, so PartnerConfig can't read them.
	// We want to see that validation fails when these are missing.
	awsKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	// And make sure we restore them...
	defer os.Setenv("AWS_ACCESS_KEY_ID", awsKey)
	defer os.Setenv("AWS_SECRET_ACCESS_KEY", awsSecret)

	// Validation should fail on missing AWS credentials
	// and/or missing receiving bucket.
	partnerConfig.AwsAccessKeyId = ""
	err := partnerConfig.Validate()
	assert.NotNil(t, err, "Validation should have failed on missing Access Key")

	partnerConfig.AwsAccessKeyId = "abc"
	partnerConfig.AwsSecretAccessKey = ""
	err = partnerConfig.Validate()
	assert.NotNil(t, err, "Validation should have failed on missing Secret Key")

	partnerConfig.AwsSecretAccessKey = "xyz"
	partnerConfig.ReceivingBucket = ""
	err = partnerConfig.Validate()
	assert.NotNil(t, err, "Validation should have failed on missing Receiving Bucket")

	partnerConfig.ReceivingBucket = "123"
	partnerConfig.RestorationBucket = ""
	err = partnerConfig.Validate()
	assert.NotNil(t, err, "Validation should have failed on missing Restoration Bucket")

	partnerConfig.RestorationBucket = "blah"
	partnerConfig.DownloadDir = ""
	err = partnerConfig.Validate()
	assert.NotNil(t, err, "Validation should have failed on missing DownloadDir")
}
