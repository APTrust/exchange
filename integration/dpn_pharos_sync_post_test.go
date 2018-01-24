package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app dpn_pharos_sync. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
dpn_pharos_sync.
*/

func TestDPNPharosSyncResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()

	pathToLogFile := filepath.Join(config.LogDirectory, "dpn_pharos_sync.log")
	require.True(t, fileutil.FileExists(pathToLogFile))

	data, err := ioutil.ReadFile(pathToLogFile)
	require.Nil(t, err)

	log := string(data)
	assert.False(t, strings.Contains(log, "ERROR"))
	assert.True(t, strings.Contains(log, "Caching institutions"))
	assert.True(t, strings.Contains(log, "/api/v2/institutions/?page=1&per_page=100"))
	assert.True(t, strings.Contains(log, "Getting latest timestamp from Pharos"))
	assert.True(t, strings.Contains(log, "/api/v2/dpn_bags/?sort=dpn_updated_at+DESC"))
	assert.True(t, strings.Contains(log, "Most recent DPN bag has update timestamp of 2015-01-21T09:18:00Z"))
	assert.True(t, strings.Contains(log, "http://localhost:3001/api-v2/bag/?after=2015-01-21T09%3A18%3A00Z&ingest_node=aptrust&page=1&page_size=100"))
	assert.True(t, strings.Contains(log, "Request returned 1 bags"))
	assert.True(t, strings.Contains(log, "/api/v2/dpn_bags/?dpn_identifier=00000000-0000-4000-a000-000000000001&page=1&page_size=10"))
	assert.True(t, strings.Contains(log, "Saved DPN Bag 00000000-0000-4000-a000-000000000001 with id"))

}
