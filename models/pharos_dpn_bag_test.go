package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPharosDPNBagSerializeForPharos(t *testing.T) {
	timestamp, _ := time.Parse(time.RFC3339, "2018-01-23T15:33:00+00:00")
	bag := models.PharosDPNBag{
		Id:               100,
		InstitutionId:    6,
		ObjectIdentifier: "test.edu/test_bag",
		DPNIdentifier:    "1ee00bbf-b39e-4302-b68a-29a40c0af025",
		DPNSize:          int64(1492),
		Node1:            "aptrust",
		Node2:            "chron",
		Node3:            "hathi",
		DPNCreatedAt:     timestamp,
		DPNUpdatedAt:     timestamp,
		CreatedAt:        timestamp,
		UpdatedAt:        timestamp,
	}
	jsonData, err := bag.SerializeForPharos()
	require.Nil(t, err)
	expected := `{"dpn_bag":{"id":100,"institution_id":6,"object_identifier":"test.edu/test_bag","dpn_identifier":"1ee00bbf-b39e-4302-b68a-29a40c0af025","dpn_size":1492,"node_1":"aptrust","node_2":"chron","node_3":"hathi","dpn_created_at":"2018-01-23T15:33:00Z","dpn_updated_at":"2018-01-23T15:33:00Z","created_at":"2018-01-23T15:33:00Z","updated_at":"2018-01-23T15:33:00Z"}}`
	assert.Equal(t, expected, string(jsonData))
}
