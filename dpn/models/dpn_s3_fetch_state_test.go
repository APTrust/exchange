package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	//apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	//"time"
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
	LocalPath:     "/mnt/lvm/dpn/restore/" + testutil.EMPTY_UUID,
}

var s3FetchJson = ``

func DPNS3FetchStateFromJson(t *testing.T) {

}

func DPNS3FetchStateToJson(t *testing.T) {
	jsonString, err := s3FetchModel.ToJson()
	require.Nil(t, err)
	assert.Equal(t, "", jsonString)
}
