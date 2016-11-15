package models_test

import (
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
		Node:        "dpn.aptrust.org",
		Task:        "Replication",
		Identifier:  "1234-5678",
		QueuedAt:    &timestamp,
		CompletedAt: &timestamp,
		Note:        &note,
		State:       &state,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
	data, err := item.SerializeForPharos()
	require.Nil(t, err)
	expected := `{"dpn_work_item":{"node":"dpn.aptrust.org","task":"Replication","identifier":"1234-5678","queued_at":"2016-11-15T15:33:00Z","completed_at":"2016-11-15T15:33:00Z","note":"All done","state":"Nebraska"}}`
	assert.Equal(t, expected, string(data))
}
