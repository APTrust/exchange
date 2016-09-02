package models_test

import (
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
func getSimpleDirConfig() (*models.Config) {
	return &models.Config{
		TarDirectory: "~/tmp/tar",
		LogDirectory: "~/tmp/log",
		RestoreDirectory: "~/tmp/restore",
		ReplicationDirectory: "~/tmp/replication",
		DPN: models.DPNConfig{
			LogDirectory: "~/tmp/dpn_logs",
			RemoteNodeHomeDirectory: "~/tmp/dpn_home",
			StagingDirectory: "~/tmp/dpn_staging",
		},
	}
}

func TestEnsureLogDirectory(t *testing.T) {
	config := &models.Config{
		TarDirectory: "~/tmp/tar",
		LogDirectory: "~/tmp/log",
		RestoreDirectory: "~/tmp/restore",
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
	assert.Equal(t, "10s", config.PrepareWorker.HeartbeatInterval)
	assert.Equal(t, 18, len(config.ReceivingBuckets))
	assert.Equal(t, configFile, config.ActiveConfig)
	assert.Equal(t, 24, config.BucketReaderCacheHours)
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
	assert.True(t, strings.HasPrefix(config.DPN.LogDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.RemoteNodeHomeDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.DPN.StagingDirectory, "/"))
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
