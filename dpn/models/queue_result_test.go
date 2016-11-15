package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewQueueItem(t *testing.T) {
	item := models.NewQueueItem("aptrust")
	require.NotNil(t, item)
	assert.Equal(t, "aptrust", item.Identifier)
}

func TestNewQueueResult(t *testing.T) {
	result := models.NewQueueResult()
	require.NotNil(t, result)
	assert.NotNil(t, result.Replications)
	assert.NotNil(t, result.Restores)
	assert.NotNil(t, result.Ingests)
	assert.NotNil(t, result.Errors)
}

func TestAddAndFindReplication(t *testing.T) {
	result := models.NewQueueResult()
	require.NotNil(t, result)
	assert.Empty(t, result.Replications)
	result.AddReplication(models.NewQueueItem("replication1"))
	assert.Equal(t, 1, len(result.Replications))
	item := result.FindReplication("replication1")
	require.NotNil(t, item)
	assert.Equal(t, "replication1", item.Identifier)
	assert.Nil(t, result.FindReplication("does not exist"))
}

func TestAddAndFindRestore(t *testing.T) {
	result := models.NewQueueResult()
	require.NotNil(t, result)
	assert.Empty(t, result.Restores)
	result.AddRestore(models.NewQueueItem("restore1"))
	assert.Equal(t, 1, len(result.Restores))
	item := result.FindRestore("restore1")
	require.NotNil(t, item)
	assert.Equal(t, "restore1", item.Identifier)
	assert.Nil(t, result.FindRestore("does not exist"))
}

func TestAddAndFindIngest(t *testing.T) {
	result := models.NewQueueResult()
	require.NotNil(t, result)
	assert.Empty(t, result.Ingests)
	result.AddIngest(models.NewQueueItem("ingest1"))
	assert.Equal(t, 1, len(result.Ingests))
	item := result.FindIngest("ingest1")
	require.NotNil(t, item)
	assert.Equal(t, "ingest1", item.Identifier)
	assert.Nil(t, result.FindIngest("does not exist"))
}

func TestAddAndHasErrors(t *testing.T) {
	result := models.NewQueueResult()
	require.NotNil(t, result)
	assert.Empty(t, result.Errors)
	assert.False(t, result.HasErrors())
	result.AddError("Oops!")
	assert.True(t, result.HasErrors())
}
