package models_test

import (
	"fmt"
	"github.com/APTrust/exchange/dpn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)


func TestNewSyncResult(t *testing.T) {
	result := dpn.NewSyncResult("aptrust")
	require.NotNil(t, result)
	assert.Equal(t, "aptrust", result.NodeName)
	assert.NotNil(t, result.FetchCounts)
	assert.NotNil(t, result.SyncCounts)
	assert.NotNil(t, result.Errors)
}

func TestAddToFetchCount(t *testing.T) {
	result := dpn.NewSyncResult("aptrust")
	require.NotNil(t, result)
	result.AddToFetchCount(dpn.DPNTypeBag, 1)
	assert.Equal(t, 1, result.FetchCounts[dpn.DPNTypeBag])
	result.AddToFetchCount(dpn.DPNTypeBag, 4)
	assert.Equal(t, 5, result.FetchCounts[dpn.DPNTypeBag])
}

func TestAddToSyncCount(t *testing.T) {
	result := dpn.NewSyncResult("aptrust")
	require.NotNil(t, result)
	result.AddToSyncCount(dpn.DPNTypeBag, 1)
	assert.Equal(t, 1, result.SyncCounts[dpn.DPNTypeBag])
	result.AddToSyncCount(dpn.DPNTypeBag, 4)
	assert.Equal(t, 5, result.SyncCounts[dpn.DPNTypeBag])
}

func TestAddError(t *testing.T) {
	result := dpn.NewSyncResult("aptrust")
	require.NotNil(t, result)
	result.AddError(dpn.DPNTypeBag, fmt.Errorf("Error 1"))
	assert.Equal(t, 1, len(result.Errors[dpn.DPNTypeBag]))
	assert.Equal(t, "Error 1", result.Errors[dpn.DPNTypeBag][0].Error())
	result.AddError(dpn.DPNTypeBag, fmt.Errorf("Error 2"))
	assert.Equal(t, 2, len(result.Errors[dpn.DPNTypeBag]))
	assert.Equal(t, "Error 2", result.Errors[dpn.DPNTypeBag][1].Error())
}

func TestHasErrors(t *testing.T) {
	result := dpn.NewSyncResult("aptrust")
	require.NotNil(t, result)
	assert.False(t, result.HasErrors(dpn.DPNTypeBag))
	assert.False(t, result.HasErrors(""))

	result.AddError(dpn.DPNTypeBag, fmt.Errorf("Error 1"))
	assert.Equal(t, 1, len(result.Errors[dpn.DPNTypeBag]))
	assert.True(t, result.HasErrors(dpn.DPNTypeBag))
	assert.True(t, result.HasErrors(""))
}
