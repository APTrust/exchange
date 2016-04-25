package config_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureLogDirectory(t *testing.T) {
	config := &config.Config{
		LogDirectory: "~/tmp/log",
	}
	absPathToLogDir, err := config.EnsureLogDirectory()
	require.Nil(t, err)
	assert.True(t, strings.HasPrefix(absPathToLogDir, "/"))
	assert.True(t, len(config.LogDirectory) >= 9)
}

func TestLoad(t *testing.T) {
	configFile := filepath.Join("testdata", "config.json")
	_, err := config.Load(configFile, "test")
	require.Nil(t, err)

}

func TestEnsurePharosConfig(t *testing.T) {

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
