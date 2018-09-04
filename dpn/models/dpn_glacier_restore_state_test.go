package models_test

import (
	dpn_models "github.com/APTrust/exchange/dpn/models"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const DPNGlacierRestoreJson = `{"DPNBag":{"uuid":"b146d1b7-9ffd-4f6f-90e9-ca5f1aa486c0","local_id":"999-999","member":"9a000000-0000-4000-a000-000000000001","size":12345678,"first_version_uuid":"b146d1b7-9ffd-4f6f-90e9-ca5f1aa486c0","version":1,"ingest_node":"tdr","admin_node":"tdr","bag_type":"D","rights":[],"interpretive":[],"replicating_nodes":[],"message_digests":null,"fixity_checks":null,"created_at":"2018-06-13T08:16:54Z","updated_at":"2018-06-13T08:16:54Z"},"GlacierBucket":"test.dpn.buckeypoo","GlacierKey":"b146d1b7-9ffd-4f6f-90e9-ca5f1aa486c0","RequestAccepted":true,"RequestedAt":"2018-09-04T16:16:54Z","AttemptNumber":3,"EstimatedDeletionFromS3":"2018-09-07T16:16:54Z","IsAvailableInS3":true,"ErrorMessage":""}`

func TestDPNGlacierRestoreStateFromJson(t *testing.T) {
	state := getTestDPNGlacierRestoreState()
	stateFromJson, err := dpn_models.DPNGlacierRestoreStateFromJson(DPNGlacierRestoreJson)
	require.Nil(t, err)
	require.NotNil(t, stateFromJson.DPNBag)
	assert.Equal(t, state.DPNBag.UUID, stateFromJson.DPNBag.UUID)
	assert.Equal(t, 3, stateFromJson.AttemptNumber)
	assert.False(t, stateFromJson.EstimatedDeletionFromS3.IsZero())
}

func TestDPNGlacierRestoreStateToJson(t *testing.T) {
	state := getTestDPNGlacierRestoreState()
	jsonStr, err := state.ToJson()
	require.Nil(t, err)
	assert.Equal(t, DPNGlacierRestoreJson, jsonStr)
}

func getTestDPNGlacierRestoreState() *dpn_models.DPNGlacierRestoreState {
	timestamp, _ := time.Parse(time.RFC3339, "2018-09-04T16:16:54Z")
	expectedDeletion := timestamp.Add(time.Hour * time.Duration(72))
	queuedAt := timestamp.Add(time.Hour * time.Duration(-1))
	createdAt := timestamp.Add(time.Hour * time.Duration(-2))
	bagCreatedAt := timestamp.Add(time.Hour * time.Duration(-2000))
	note := "It's Hammer Time."
	bagUUID := "b146d1b7-9ffd-4f6f-90e9-ca5f1aa486c0"
	dpnWorkItem := &apt_models.DPNWorkItem{
		Id:          8888,
		RemoteNode:  "tdr",
		Task:        "fixity",
		Identifier:  bagUUID,
		QueuedAt:    &queuedAt,
		CompletedAt: nil,
		Note:        &note,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
	dpnBag := &dpn_models.DPNBag{
		UUID:             bagUUID,
		Interpretive:     []string{},
		Rights:           []string{},
		ReplicatingNodes: []string{},
		LocalId:          "999-999",
		Size:             12345678,
		FirstVersionUUID: bagUUID,
		Version:          1,
		BagType:          "D",
		IngestNode:       "tdr",
		AdminNode:        "tdr",
		Member:           "9a000000-0000-4000-a000-000000000001",
		CreatedAt:        bagCreatedAt,
		UpdatedAt:        bagCreatedAt,
	}
	return &dpn_models.DPNGlacierRestoreState{
		NSQMessage:              testutil.MakeNsqMessage("blah blah blah"),
		DPNWorkItem:             dpnWorkItem,
		DPNBag:                  dpnBag,
		GlacierBucket:           "test.dpn.buckeypoo",
		GlacierKey:              bagUUID,
		RequestAccepted:         true,
		RequestedAt:             timestamp,
		AttemptNumber:           3,
		EstimatedDeletionFromS3: expectedDeletion,
		IsAvailableInS3:         true,
		ErrorMessage:            "",
	}
}
