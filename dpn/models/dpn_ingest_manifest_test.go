package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNewDPNIngestManifest(t *testing.T) {
	nsqMessage := testutil.MakeNsqMessage("999")
	manifest := models.NewDPNIngestManifest(nsqMessage)
	assert.Equal(t, nsqMessage, manifest.NsqMessage)
	assert.NotNil(t, manifest.PackageSummary)
	assert.NotNil(t, manifest.ValidateSummary)
	assert.NotNil(t, manifest.StoreSummary)
	assert.NotNil(t, manifest.RecordSummary)
}

func TestManifestBuildReplicationTransfer(t *testing.T) {
	nsqMessage := testutil.MakeNsqMessage("999")
	manifest := models.NewDPNIngestManifest(nsqMessage)
	fromNode := "aptrust"
	toNode := "chron"
	link := "dpn.chron@dpn.aptrust.org:outbound/1234567.tar"
	xfer, err := manifest.BuildReplicationTransfer(fromNode, toNode, link)
	// No DPNBag attached to manifest. Should get error.
	require.NotNil(t, err)

	manifest.DPNBag = testutil.MakeDPNBag()
	xfer, err = manifest.BuildReplicationTransfer(fromNode, toNode, link)
	require.Nil(t, err)
	require.NotNil(t, xfer)

	assert.Equal(t, fromNode, xfer.FromNode)
	assert.Equal(t, toNode, xfer.ToNode)
	assert.Equal(t, manifest.DPNBag.UUID, xfer.Bag)
	assert.True(t, util.LooksLikeUUID(xfer.ReplicationId))

	assert.Equal(t, constants.AlgSha256, xfer.FixityAlgorithm)
	assert.Nil(t, xfer.FixityNonce)
	assert.Nil(t, xfer.FixityValue)
	assert.Equal(t, "rsync", xfer.Protocol)
	assert.Equal(t, link, xfer.Link)
	assert.False(t, xfer.CreatedAt.IsZero())
	assert.False(t, xfer.UpdatedAt.IsZero())
}

func TestManifestBuildDPNIngestEvent(t *testing.T) {
	nsqMessage := testutil.MakeNsqMessage("999")
	manifest := models.NewDPNIngestManifest(nsqMessage)
	manifest.StorageURL = "https://example.com/my_bag.tar"

	// Should give error, since DPNBag is nil
	event, err := manifest.BuildDPNIngestEvent()
	require.NotNil(t, err)

	manifest.DPNBag = testutil.MakeDPNBag()
	event, err = manifest.BuildDPNIngestEvent()
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Item ingested into DPN", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, manifest.StorageURL, event.OutcomeDetail)
	assert.Equal(t, "APTrust exchange", event.Object)
	assert.Equal(t, "https://github.com/APTrust/exchange", event.Agent)
	assert.True(t, strings.Contains(event.OutcomeInformation, manifest.StorageURL))
}

func TestManifestBuildDPNIdentifierEvent(t *testing.T) {
	nsqMessage := testutil.MakeNsqMessage("999")
	manifest := models.NewDPNIngestManifest(nsqMessage)
	manifest.StorageURL = "https://example.com/my_bag.tar"

	// Should give error, since DPNBag is nil
	event, err := manifest.BuildDPNIdentifierEvent()
	require.NotNil(t, err)

	manifest.DPNBag = testutil.MakeDPNBag()
	event, err = manifest.BuildDPNIdentifierEvent()
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Item assigned DPN UUID", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, manifest.DPNBag.UUID, event.OutcomeDetail)
	assert.Equal(t, "APTrust exchange using Satori go.uuid", event.Object)
	assert.Equal(t, "https://github.com/satori/go.uuid", event.Agent)
	assert.True(t, strings.Contains(event.OutcomeInformation, manifest.DPNBag.UUID))

}
