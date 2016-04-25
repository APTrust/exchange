package config_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureLogDirectory(t *testing.T) {
	config := &config.Config{
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
	configFile := filepath.Join("testdata", "config.json")
	appConfig, err := config.Load(configFile, "test")
	require.Nil(t, err)

	// Spot check a few settings.
	assert.Equal(t, 60, appConfig.MaxDaysSinceFixityCheck)
	assert.Equal(t, "http://localhost:3000", appConfig.PharosURL)
	assert.Equal(t, "10s", appConfig.PrepareWorker.HeartbeatInterval)
	assert.Equal(t, 18, len(appConfig.ReceivingBuckets))
}

func TestEnsurePharosConfig(t *testing.T) {
	configFile := filepath.Join("testdata", "config.json")
	appConfig, err := config.Load(configFile, "test")
	require.Nil(t, err)

	url := appConfig.PharosURL
	appConfig.PharosURL = ""
	err = appConfig.EnsurePharosConfig()
	assert.Equal(t, "PharosUrl is missing from config file", err.Error())

	appConfig.PharosURL = url
	apiUser := os.Getenv("PHAROS_API_USER")
	os.Setenv("PHAROS_API_USER", "")
	err = appConfig.EnsurePharosConfig()
	assert.Equal(t, "Environment variable PHAROS_API_USER is not set", err.Error())

	os.Setenv("PHAROS_API_USER", "Bogus value set by config_test.go")
	apiKey := os.Getenv("PHAROS_API_KEY")
	os.Setenv("PHAROS_API_KEY", "")
	err = appConfig.EnsurePharosConfig()
	assert.Equal(t, "Environment variable PHAROS_API_KEY is not set", err.Error())

	os.Setenv("PHAROS_API_USER", apiUser)
	os.Setenv("PHAROS_API_KEY", apiKey)
}

func TestExpandFilePaths(t *testing.T) {
	config := &config.Config{
		TarDirectory: "~/tmp/tar",
		LogDirectory: "~/tmp/log",
		RestoreDirectory: "~/tmp/restore",
		ReplicationDirectory: "~/tmp/replication",
	}
	config.ExpandFilePaths()
	assert.True(t, strings.HasPrefix(config.TarDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.LogDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.RestoreDirectory, "/"))
	assert.True(t, strings.HasPrefix(config.ReplicationDirectory, "/"))
	assert.True(t, len(config.TarDirectory) >= 9)
	assert.True(t, len(config.LogDirectory) >= 9)
	assert.True(t, len(config.RestoreDirectory) >= 9)
	assert.True(t, len(config.ReplicationDirectory) >= 9)
}
