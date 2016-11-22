package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewDPNBag(t *testing.T) {
	bag := models.NewDPNBag("local_id", "some_member", "some_node")
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
