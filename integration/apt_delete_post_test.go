package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_file_delete. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
apt_file_delete.
*/

func TestDeleteResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	_context := context.NewContext(config)

	s3Key := testutil.INTEGRATION_GOOD_BAGS[9]
	identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
	identifier = strings.Replace(identifier, ".tar", "", 1)

	// Check delete events
	params := url.Values{}
	params.Set("object_identifier", identifier)
	params.Set("event_type", constants.EventDeletion)
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.PremisEventList(params)

	// 7 events: 6 files, plus one for the object itself.
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	assert.Equal(t, 7, len(events))

	maxKeys := int64(10)
	s3Client := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustS3Region,
		_context.Config.PreservationBucket, maxKeys)
	glacierClient := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustGlacierRegion,
		_context.Config.ReplicationBucket, maxKeys)
	// Make sure files don't exist in S3
	for _, event := range events {
		// Find the UUID for this file. That's the storage key
		// for S3 and Glacier. But ignore the object-level delete
		// event, because that's not associated with any file.
		if event.GenericFileIdentifier == "" {
			continue
		}
		resp := _context.PharosClient.GenericFileGet(event.GenericFileIdentifier, false)
		require.Nil(t, resp.Error)
		gf := resp.GenericFile()
		require.NotNil(t, gf)
		key, err := gf.PreservationStorageFileName()
		require.Nil(t, err)

		// Make sure the file's not there anymore.
		s3Client.GetList(key)
		assert.Empty(t, s3Client.Response.Contents)
		s3Client.Response.Contents = nil

		glacierClient.GetList(key)
		assert.Empty(t, glacierClient.Response.Contents)
		glacierClient.Response.Contents = nil
	}
}
