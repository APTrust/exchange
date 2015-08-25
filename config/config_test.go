package config_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
)

func TestExpandFilePaths(t *testing.T) {
	config := &bagman.Config{
		TarDirectory: "~/tmp/tar",
		LogDirectory: "~/tmp/log",
		RestoreDirectory: "~/tmp/restore",
		ReplicationDirectory: "~/tmp/replication",
	}
	config.ExpandFilePaths()
	if len(config.TarDirectory) <= 9 {
		t.Errorf("TarDirectory was not expanded: %s", config.TarDirectory)
	}
	if len(config.LogDirectory) <= 9 {
		t.Errorf("LogDirectory was not expanded: %s", config.LogDirectory)
	}
	if len(config.RestoreDirectory) <= 13 {
		t.Errorf("RestoreDirectory was not expanded: %s", config.RestoreDirectory)
	}
	if len(config.ReplicationDirectory) <= 17 {
		t.Errorf("ReplicationDirectory was not expanded: %s", config.ReplicationDirectory)
	}
}
