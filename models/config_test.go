package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Returns a simple config object with only directory names filled in.
func getSimpleDirConfig() *models.Config {
	return &models.Config{
		TarDirectory:         "~/tmp/tar",
		LogDirectory:         "~/tmp/log",
		RestoreDirectory:     "~/tmp/restore",
		ReplicationDirectory: "~/tmp/replication",
		DPN: models.DPNConfig{
			LogDirectory:            "~/tmp/dpn_logs",
			RemoteNodeHomeDirectory: "~/tmp/dpn_home",
			StagingDirectory:        "~/tmp/dpn_staging",
		},
	}
}

func TestEnsureLogDirectory(t *testing.T) {
	config := &models.Config{
		TarDirectory:         "~/tmp/tar",
		LogDirectory:         "~/tmp/log",
		RestoreDirectory:     "~/tmp/restore",
		ReplicationDirectory: "~/tmp/replication",
	}
	absPathToLogDir, err := config.EnsureLogDirectory()
	require.Nil(t, err)
	assert.True(t, strings.HasPrefix(absPathToLogDir, "/"))
	assert.True(t, len(config.LogDirectory) >= 9)
}

func TestLoad(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	// Spot check a few settings.
	assert.Equal(t, 60, config.MaxDaysSinceFixityCheck)
	assert.Equal(t, "http://localhost:3000", config.PharosURL)
	assert.Equal(t, "10s", config.FetchWorker.HeartbeatInterval)
	assert.Equal(t, 18, len(config.ReceivingBuckets))
	assert.Equal(t, configFile, config.ActiveConfig)
	assert.Equal(t, 24, config.BucketReaderCacheHours)
	assert.Equal(t, "api-v2", config.DPN.DPNAPIVersion)
	assert.Equal(t, "us-east-1", config.DPN.DPNGlacierRegion)
}

func TestEnsurePharosConfig(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	url := config.PharosURL
	config.PharosURL = ""
	err = config.EnsurePharosConfig()
	assert.Equal(t, "PharosUrl is missing from config file", err.Error())

	config.PharosURL = url
	apiUser := os.Getenv("PHAROS_API_USER")
	os.Setenv("PHAROS_API_USER", "")
	err = config.EnsurePharosConfig()
	assert.Equal(t, "Environment variable PHAROS_API_USER is not set", err.Error())

	os.Setenv("PHAROS_API_USER", "Bogus value set by config_test.go")
	apiKey := os.Getenv("PHAROS_API_KEY")
	os.Setenv("PHAROS_API_KEY", "")
	err = config.EnsurePharosConfig()
	assert.Equal(t, "Environment variable PHAROS_API_KEY is not set", err.Error())

	os.Setenv("PHAROS_API_USER", apiUser)
	os.Setenv("PHAROS_API_KEY", apiKey)
}

func TestEnsureDPNConfig(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	assert.NotEmpty(t, config.DPN.DefaultMetadata.BagItVersion)
	assert.NotEmpty(t, config.DPN.DefaultMetadata.BagItEncoding)
	assert.NotEmpty(t, config.DPN.DefaultMetadata.IngestNodeName)
	assert.NotEmpty(t, config.DPN.DefaultMetadata.IngestNodeAddress)
	assert.NotEmpty(t, config.DPN.DefaultMetadata.IngestNodeContactName)
	assert.NotEmpty(t, config.DPN.DefaultMetadata.IngestNodeContactEmail)

	assert.NotEmpty(t, config.DPN.RestClient.LocalServiceURL)
	assert.NotEmpty(t, config.DPN.RestClient.LocalAPIRoot)
	assert.NotEmpty(t, config.DPN.RestClient.LocalAuthToken)

	assert.EqualValues(t, 3, config.DPN.DPNValidationWorker.MaxAttempts)
	assert.EqualValues(t, 3, config.DPN.DPNCopyWorker.MaxAttempts)
	assert.EqualValues(t, 4, config.DPN.DPNGlacierRestoreWorker.Workers)

	assert.Equal(t, "chron_token", config.DPN.RemoteNodeAdminTokensForTesting["chron"])
	assert.Equal(t, "aptrust_token", config.DPN.RemoteNodeTokens["chron"])
	assert.Equal(t, "http://localhost:3002", config.DPN.RemoteNodeURLs["chron"])
}

func TestExpandFilePaths(t *testing.T) {
	config := getSimpleDirConfig()
	config.ExpandFilePaths()
	assert.True(t, strings.HasPrefix(config.TarDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.LogDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.RestoreDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.ReplicationDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.BagValidationConfigFile, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.LogDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.RemoteNodeHomeDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.StagingDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.BagValidationConfigFile, "/"))
	assert.True(t, len(config.DPN.LogDirectory) >= 9)
	assert.True(t, len(config.DPN.RemoteNodeHomeDirectory) >= 9)
	assert.True(t, len(config.DPN.StagingDirectory) >= 9)
	assert.True(t, len(config.TarDirectory) >= 9)
	assert.True(t, len(config.LogDirectory) >= 9)
	assert.True(t, len(config.RestoreDirectory) >= 9)
	assert.True(t, len(config.ReplicationDirectory) >= 9)
}

func TestEnsureLogDir(t *testing.T) {
	config := getSimpleDirConfig()
	absLogPath, err := config.EnsureLogDirectory()
	require.Nil(t, err)

	assert.True(t, strings.HasPrefix(absLogPath, "/"))
	assert.True(t, fileutil.FileExists(absLogPath))

	assert.True(t, fileutil.FileExists(config.TarDirectory))
	assert.True(t, fileutil.FileExists(config.LogDirectory))
	assert.True(t, fileutil.FileExists(config.RestoreDirectory))
	assert.True(t, fileutil.FileExists(config.ReplicationDirectory))
	assert.True(t, fileutil.FileExists(config.DPN.LogDirectory))
	assert.True(t, fileutil.FileExists(config.DPN.StagingDirectory))
}

func TestStorageRegionAndBucketFor(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	region, bucket, err := config.StorageRegionAndBucketFor(constants.StorageStandard)
	assert.Equal(t, config.APTrustS3Region, region)
	assert.Equal(t, config.PreservationBucket, bucket)
	assert.Nil(t, err)

	region, bucket, err = config.StorageRegionAndBucketFor(constants.StorageGlacierVA)
	assert.Equal(t, config.GlacierRegionVA, region)
	assert.Equal(t, config.GlacierBucketVA, bucket)
	assert.Nil(t, err)

	region, bucket, err = config.StorageRegionAndBucketFor(constants.StorageGlacierOH)
	assert.Equal(t, config.GlacierRegionOH, region)
	assert.Equal(t, config.GlacierBucketOH, bucket)
	assert.Nil(t, err)

	region, bucket, err = config.StorageRegionAndBucketFor(constants.StorageGlacierOR)
	assert.Equal(t, config.GlacierRegionOR, region)
	assert.Equal(t, config.GlacierBucketOR, bucket)
	assert.Nil(t, err)

	region, bucket, err = config.StorageRegionAndBucketFor("Spongebob")
	assert.Equal(t, "", region)
	assert.Equal(t, "", bucket)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Unknown Storage Option"))
}

func TestTestsAreRunning(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	assert.True(t, config.TestsAreRunning())
}

func TestGetAWSAccessKeyId(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		assert.Equal(t, os.Getenv("AWS_ACCESS_KEY_ID"), config.GetAWSAccessKeyId())
	} else {
		assert.Equal(t, "TestKeyId", config.GetAWSAccessKeyId())
	}
}

func TestGetAWSSecretAccessKey(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		assert.Equal(t, os.Getenv("AWS_SECRET_ACCESS_KEY"), config.GetAWSSecretAccessKey())
	} else {
		assert.Equal(t, "TestSecretKey", config.GetAWSSecretAccessKey())
	}
}

func TestActiveAWSStorageRegions(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	regions := config.ActiveAWSStorageRegions()

	assert.Equal(t, 7, len(regions))
	assert.Equal(t, "us-east-1", regions[constants.StorageStandard])
	assert.Equal(t, "us-east-1", regions[constants.StorageGlacierVA])
	assert.Equal(t, "us-east-2", regions[constants.StorageGlacierOH])
	assert.Equal(t, "us-west-2", regions[constants.StorageGlacierOR])
	assert.Equal(t, "us-east-1", regions[constants.StorageGlacierDeepVA])
	assert.Equal(t, "us-east-2", regions[constants.StorageGlacierDeepOH])
	assert.Equal(t, "us-west-2", regions[constants.StorageGlacierDeepOR])
}

func TestAWSS3Buckets(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	buckets := config.AWSS3Buckets()
	assert.Equal(t, 1, len(buckets))
	assert.Equal(t, "aptrust.test.preservation", buckets[constants.StorageStandard])
}

func TestAWSGlacierBuckets(t *testing.T) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	buckets := config.AWSGlacierBuckets()
	assert.Equal(t, 7, len(buckets))
	assert.Equal(t, "aptrust.test.preservation.oregon", buckets[constants.StorageStandard])
	assert.Equal(t, "aptrust.test.preservation.glacier.va", buckets[constants.StorageGlacierVA])
	assert.Equal(t, "aptrust.test.preservation.glacier.oh", buckets[constants.StorageGlacierOH])
	assert.Equal(t, "aptrust.test.preservation.glacier.or", buckets[constants.StorageGlacierOR])
	assert.Equal(t, "aptrust.test.preservation.glacier-deep.va", buckets[constants.StorageGlacierDeepVA])
	assert.Equal(t, "aptrust.test.preservation.glacier-deep.oh", buckets[constants.StorageGlacierDeepOH])
	assert.Equal(t, "aptrust.test.preservation.glacier-deep.or", buckets[constants.StorageGlacierDeepOR])
}
