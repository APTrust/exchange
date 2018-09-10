package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var s3FetchModel = &models.DPNS3FetchState{
	DPNBag: &models.DPNBag{
		UUID:             testutil.EMPTY_UUID,
		LocalId:          "Ned Flanders",
		Member:           testutil.EMPTY_UUID,
		Size:             uint64(12345),
		FirstVersionUUID: testutil.EMPTY_UUID,
		Version:          uint32(1),
		IngestNode:       "aptrust",
		AdminNode:        "aptrust",
		BagType:          "D",
		Rights:           make([]string, 0),
		Interpretive:     make([]string, 0),
		ReplicatingNodes: make([]string, 0),
		MessageDigests:   make([]*models.MessageDigest, 0),
		FixityChecks:     make([]*models.FixityCheck, 0),
		CreatedAt:        testutil.TEST_TIMESTAMP,
		UpdatedAt:        testutil.TEST_TIMESTAMP,
	},
	S3Bucket:      "aptrust.dpn.test.restore",
	S3Key:         testutil.EMPTY_UUID,
	StartedAt:     testutil.TEST_TIMESTAMP,
	CompletedAt:   testutil.TEST_TIMESTAMP,
	AttemptNumber: 4,
	ErrorMessage:  "",
	ErrorIsFatal:  false,
	LocalPath:     "/mnt/lvm/dpn/restore/" + testutil.EMPTY_UUID + ".tar",
}

var s3FetchJson = `{"DPNBag":{"uuid":"00000000-0000-0000-0000-000000000000","local_id":"Ned Flanders","member":"00000000-0000-0000-0000-000000000000","size":12345,"first_version_uuid":"00000000-0000-0000-0000-000000000000","version":1,"ingest_node":"aptrust","admin_node":"aptrust","bag_type":"D","rights":[],"interpretive":[],"replicating_nodes":[],"message_digests":[],"fixity_checks":[],"created_at":"2016-06-16T10:24:16Z","updated_at":"2016-06-16T10:24:16Z"},"S3Bucket":"aptrust.dpn.test.restore","S3Key":"00000000-0000-0000-0000-000000000000","StartedAt":"2016-06-16T10:24:16Z","CompletedAt":"2016-06-16T10:24:16Z","AttemptNumber":4,"ErrorMessage":"","ErrorIsFatal":false,"LocalPath":"/mnt/lvm/dpn/restore/00000000-0000-0000-0000-000000000000.tar"}`

func TestDPNS3FetchStateFromJson(t *testing.T) {
	obj, err := models.DPNS3FetchStateFromJson(s3FetchJson)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, testutil.EMPTY_UUID, obj.DPNBag.UUID)
	assert.Equal(t, "D", obj.DPNBag.BagType)
	assert.Equal(t, "aptrust.dpn.test.restore", obj.S3Bucket)
	assert.Equal(t, testutil.EMPTY_UUID, obj.S3Key)
	assert.Equal(t, "/mnt/lvm/dpn/restore/00000000-0000-0000-0000-000000000000.tar", obj.LocalPath)
}

func TestDPNS3FetchStateToJson(t *testing.T) {
	jsonString, err := s3FetchModel.ToJson()
	require.Nil(t, err)
	assert.Equal(t, s3FetchJson, jsonString)
}
