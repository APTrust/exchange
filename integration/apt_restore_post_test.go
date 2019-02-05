package integration_test

import (
	"bufio"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_restore. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
apt_restore.
*/

func TestRetoreResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()

	// Find the log file that apt_store created when it was running
	// with the "config/integration.json" config options. We'll read
	// that file. The bags we're checking are the same ones we marked
	// for restore in integration/apt_mark_for_restore_test.go.
	testFailed := false
	pathToJsonLog := filepath.Join(config.LogDirectory, "apt_restore.json")
	for _, bagName := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		objIdentifier := strings.Replace(bagName, "aptrust.integration.test", "test.edu", 1)
		objIdentifier = strings.Replace(objIdentifier, ".tar", "", 1)
		restoreState, err := testutil.FindRestoreStateInLog(pathToJsonLog, objIdentifier)
		assert.Nil(t, err)
		if err != nil {
			testFailed = true
			continue
		}
		// TODO: Test WorkItem (stage, status, etc.) in Pharos
		// TODO: Check for file existence in S3
		restoreTestCommon(t, objIdentifier, restoreState, config)
	}

	files := []string{
		"test.edu/example.edu.tagsample_good/data/datastream-DC",
		"test.edu/example.edu.tagsample_good/data/datastream-MARC",
	}
	pathToJsonLog = filepath.Join(config.LogDirectory, "apt_file_restore.json")
	for _, gfIdentifier := range files {
		restoreState, err := testutil.FindFileRestoreStateInLog(pathToJsonLog, gfIdentifier)
		assert.Nil(t, err)
		if err != nil {
			testFailed = true
			continue
		}
		assert.False(t, restoreState.RestoreSummary.FinishedAt.IsZero(),
			"FinishedAt should be non-empty for %s", gfIdentifier)
		assert.Empty(t, restoreState.RestoreSummary.Errors,
			"Errors should be empty for %s", gfIdentifier)
		assert.False(t, restoreState.CopiedToRestorationAt.IsZero(),
			"CopiedToRestorationAt should be non-empty for %s", gfIdentifier)
		assert.NotEmpty(t, restoreState.RestoredToURL,
			"RestoredToURL should not be empty for %s", gfIdentifier)

	}

	// This happens last...
	for _, bagName := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		objIdentifier := strings.Replace(bagName, "aptrust.integration.test", "test.edu", 1)
		objIdentifier = strings.Replace(objIdentifier, ".tar", "", 1)
		assert.True(t, emailSentFor(objIdentifier, config),
			"Did not send restoration spot test email for %s", objIdentifier)
	}

	require.False(t, testFailed, "One or more tests failed")
}

func restoreTestCommon(t *testing.T, objIdentifier string, restoreState *models.RestoreState, config *models.Config) {
	// Make sure all stages ran to completion
	assert.False(t, restoreState.PackageSummary.FinishedAt.IsZero(),
		"PackageSummary.FinishedAt should be non-empty for %s", objIdentifier)
	assert.False(t, restoreState.ValidateSummary.FinishedAt.IsZero(),
		"ValidateSummary.FinishedAt should be non-empty for %s", objIdentifier)
	assert.False(t, restoreState.CopySummary.FinishedAt.IsZero(),
		"CopySummary.FinishedAt should not non-empty for %s", objIdentifier)
	assert.False(t, restoreState.RecordSummary.FinishedAt.IsZero(),
		"RecordSummary.FinishedAt should be non-empty for %s", objIdentifier)

	assert.Empty(t, restoreState.PackageSummary.Errors,
		"PackageSummary.Errors should be empty for %s", objIdentifier)
	assert.Empty(t, restoreState.ValidateSummary.Errors,
		"ValidateSummary.Errors should be empty for %s", objIdentifier)
	assert.Empty(t, restoreState.CopySummary.Errors,
		"CopySummary.Errors should be empty for %s", objIdentifier)
	assert.Empty(t, restoreState.RecordSummary.Errors,
		"RecordSummary.Errors should be empty for %s", objIdentifier)

	assert.True(t, restoreState.PackageSummary.Retry,
		"PackageSummary.Retry should be true for %s", objIdentifier)
	assert.True(t, restoreState.ValidateSummary.Retry,
		"ValidateSummary.Retry should be true for %s", objIdentifier)
	assert.True(t, restoreState.CopySummary.Retry,
		"CopySummary.Retry should be true for %s", objIdentifier)
	assert.True(t, restoreState.RecordSummary.Retry,
		"RecordSummary.Retry should be true for %s", objIdentifier)

	assert.NotEmpty(t, restoreState.LocalBagDir,
		"LocalBagDir should not be empty for %s", objIdentifier)
	assert.NotEmpty(t, restoreState.LocalTarFile,
		"LocalTarFile should not be empty for %s", objIdentifier)
	assert.NotEmpty(t, restoreState.RestoredToUrl,
		"RestoredToUrl should not be empty for %s", objIdentifier)

	assert.False(t, restoreState.CopiedToRestorationAt.IsZero(),
		"CopiedToRestorationAt should not be empty for %s", objIdentifier)
	assert.False(t, restoreState.BagDirDeletedAt.IsZero(),
		"BagDirDeletedAt should not be empty for %s", objIdentifier)
	assert.False(t, restoreState.TarFileDeletedAt.IsZero(),
		"TarFileDeletedAt should not be empty for %s", objIdentifier)
}

// Did we record sending a restoration spot test email for this item?
func emailSentFor(objIdentifier string, config *models.Config) bool {
	file, _ := os.Open(filepath.Join(config.LogDirectory, "apt_restore.log"))
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "Sent restoration spot test email") &&
			strings.Contains(line, objIdentifier) {
			return true
		}
	}
	return false
}
