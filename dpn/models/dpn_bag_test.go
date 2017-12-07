package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNewDPNBag(t *testing.T) {
	bag, err := models.NewDPNBag("local_id", "not_a_uuid", "some_member", "some_node")
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "does not look like a valid uuid"))
	assert.Nil(t, bag)

	_uuid := uuid.NewV4().String()
	bag, err = models.NewDPNBag("local_id", _uuid, "some_member", "some_node")
	assert.Nil(t, err)
	assert.NotNil(t, bag)
	assert.NotEmpty(t, bag.UUID)
	assert.Equal(t, bag.UUID, bag.FirstVersionUUID)
	assert.Equal(t, "local_id", bag.LocalId)
	assert.Equal(t, "some_member", bag.Member)
	assert.Equal(t, "some_node", bag.AdminNode)
	assert.Equal(t, "some_node", bag.IngestNode)
	assert.EqualValues(t, 1, bag.Version)
	assert.Equal(t, "D", bag.BagType)
	assert.NotNil(t, bag.MessageDigests)
	assert.Empty(t, bag.MessageDigests)
	assert.NotNil(t, bag.Interpretive)
	assert.Empty(t, bag.Interpretive)
	assert.NotNil(t, bag.Rights)
	assert.Empty(t, bag.Rights)
	assert.NotNil(t, bag.ReplicatingNodes)
	assert.Empty(t, bag.ReplicatingNodes)
	assert.False(t, bag.CreatedAt.IsZero())
	assert.False(t, bag.UpdatedAt.IsZero())
}
