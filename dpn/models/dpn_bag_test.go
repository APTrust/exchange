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
}
