package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSerializeDPNWorkItemForPharos(t *testing.T) {
	timestamp, _ := time.Parse(time.RFC3339, "2016-11-15T15:33:00+00:00")
	note := "All done"
	state := "Nebraska"
	item := models.DPNWorkItem{
		Id:          1000,
		RemoteNode:  "chron",
		Task:        "Replication",
		Identifier:  "1234-5678",
		QueuedAt:    &timestamp,
		CompletedAt: &timestamp,
		Note:        &note,
		State:       &state,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
		Retry:       true,
		Stage:       constants.StageRequested,
		Status:      constants.StatusPending,
	}
	data, err := item.SerializeForPharos()
	require.Nil(t, err)

	// TODO: expected will change to include retry, status, and stage
	// when Pharos understands those things.
	// See: https://www.pivotaltracker.com/story/show/160263632
	// And: https://www.pivotaltracker.com/story/show/160287414
	expected := `{"dpn_work_item":{"remote_node":"chron","task":"Replication","identifier":"1234-5678","queued_at":"2016-11-15T15:33:00Z","completed_at":"2016-11-15T15:33:00Z","processing_node":null,"pid":0,"note":"All done","state":"Nebraska"}}`
	assert.Equal(t, expected, string(data))
}

func TestDPNWorkItemIsBeingProcessed(t *testing.T) {
	item := models.DPNWorkItem{}
	assert.False(t, item.IsBeingProcessed())

	processingNode := "example.com"
	item.ProcessingNode = &processingNode
	item.Pid = 900
	assert.True(t, item.IsBeingProcessed())
}

func TestDPNWorkItemIsBeingProcessedByMe(t *testing.T) {
	item := models.DPNWorkItem{}
	assert.False(t, item.IsBeingProcessedByMe("aptrust.org", 1234))

	processingNode := "example.com"
	item.ProcessingNode = &processingNode
	item.Pid = 900
	assert.False(t, item.IsBeingProcessedByMe("aptrust.org", 1234))
	assert.True(t, item.IsBeingProcessedByMe("example.com", 900))
}

func TestDPNWorkItemSetNodeAndPid(t *testing.T) {
	item := models.DPNWorkItem{}
	assert.Nil(t, item.ProcessingNode)
	assert.Equal(t, 0, item.Pid)
	item.SetNodeAndPid()
	assert.NotEmpty(t, item.ProcessingNode)
	assert.NotEqual(t, 0, item.Pid)
}

func TestDPNWorkItemClearNodeAndPid(t *testing.T) {
	item := models.DPNWorkItem{}
	item.SetNodeAndPid()
	assert.NotEmpty(t, item.ProcessingNode)
	assert.NotEqual(t, 0, item.Pid)
	item.ClearNodeAndPid()
	assert.Nil(t, item.ProcessingNode)
	assert.Equal(t, 0, item.Pid)
}

func TestDPNWorkItemIsCompletedOrCancelled(t *testing.T) {
	item := models.DPNWorkItem{}
	item.Status = constants.StatusStarted
	assert.False(t, item.IsCompletedOrCancelled())
	item.Status = constants.StatusPending
	assert.False(t, item.IsCompletedOrCancelled())
	item.Status = constants.StatusSuccess
	assert.True(t, item.IsCompletedOrCancelled())
	item.Status = constants.StatusFailed
	assert.False(t, item.IsCompletedOrCancelled())
	item.Status = constants.StatusCancelled
	assert.True(t, item.IsCompletedOrCancelled())
}
