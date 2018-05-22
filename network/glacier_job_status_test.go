package network_test

import (
	"github.com/APTrust/exchange/network"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestGlacierStatus(t *testing.T) {
	// canTestS3 is defined in s3_download_test
	if !canTestS3() {
		return
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	client := network.NewGlacierJobStatus(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.GlacierRegionVA)
	result, err := client.GetStatus(_context.Config.GlacierBucketVA, "TestJobId")

	// This sucks. At the moment, we can only test whether
	// AWS received the request, since we don't have a valid
	// Glacier Job Id.
	//
	// Just get rid of this...
	assert.NotNil(t, err)
	assert.NotNil(t, result)
}
