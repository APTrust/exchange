package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_fixity_check. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
the apt_fixity_check.
*/

func TestFixityCheckResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	_context := context.NewContext(config)

	expectedFiles := []string{
		"test.edu/ncsu.1840.16-1004/data/metadata.xml",
		"test.edu/ncsu.1840.16-1004/data/object.properties",
		"test.edu/ncsu.1840.16-1004/data/ORIGINAL/1",
		"test.edu/ncsu.1840.16-1004/data/ORIGINAL/1-metadata.xml",
		"test.edu/ncsu.1840.16-1004/aptrust-info.txt",
		"test.edu/ncsu.1840.16-1004/bag-info.txt",
		"test.edu/ncsu.1840.16-1005/data/metadata.xml",
		"test.edu/ncsu.1840.16-1005/data/object.properties",
		"test.edu/ncsu.1840.16-1005/data/ORIGINAL/1",
		"test.edu/ncsu.1840.16-1005/data/ORIGINAL/1-metadata.xml",
		"test.edu/ncsu.1840.16-1005/aptrust-info.txt",
		"test.edu/ncsu.1840.16-1005/bag-info.txt",
	}

	// Each fixity check should result in a fixity check event for the file.
	for _, file := range expectedFiles {
		params := url.Values{}
		params.Set("file_identifier", file)
		params.Set("event_type", constants.EventFixityCheck)
		params.Set("page", "1")
		params.Set("per_page", "10")
		resp := _context.PharosClient.PremisEventList(params)
		require.Nil(t, resp.Error)
		events := resp.PremisEvents()

		// Should be two fixity check events. 1 against ingest manifest
		// md5 digest, which occurs on ingest, and the other against
		// sha256 digest, which is the periodic (90 day) fixity check.
		assert.Equal(t, 2, len(events), file)

		// event.OutcomeDetail contains the actual checksum, with the
		// algorithm as prefix. For example:
		// sha256:ab527760f4e31acb7a8812eb3dd443bc9f47e04c4968e4c1966ec9d1e668540f
		foundSha256 := false
		for _, event := range events {
			if strings.Contains(event.OutcomeDetail, "sha256") {
				foundSha256 = true
			}
		}
		assert.True(t, foundSha256, file)
	}
}

func TestFixityCheckAddRemove(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}

	// Bonus: check the log file to see if the fixity checker is
	// properly tracking which files it's working on.
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	logFile := filepath.Join(config.LogDirectory, "apt_fixity_check.log")

	// 10 files should have been added to the ItemsInProcess sync map
	cmd := exec.Command("grep", "ncsu", logFile, "| grep Added | wc -l")
	err = cmd.Run()
	if err == nil {
		bytes, _ := cmd.Output()
		output := strings.TrimSpace(string(bytes))
		assert.Equal(t, "10", output, "Expected 10 adds, got %s", output)
	}

	// 10 files should have been removed from the ItemsInProcess sync map
	cmd = exec.Command("grep", "ncsu", logFile, "| grep Removed | wc -l")
	err = cmd.Run()
	if err == nil {
		bytes, _ := cmd.Output()
		output := strings.TrimSpace(string(bytes))
		assert.Equal(t, "10", output, "Expected 10 removes, got %s", output)
	}
}
