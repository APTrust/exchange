package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewIntellectualObject(t *testing.T) {
	obj := models.NewIntellectualObject()
	assert.NotNil(t, obj.GenericFiles)
	assert.NotNil(t, obj.PremisEvents)
	assert.NotNil(t, obj.IngestFilesIgnored)
	assert.NotNil(t, obj.IngestTags)
}

func TestTotalFileSize(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}

func TestSerializeObjectForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	data, err := intelObj.SerializeForPharos()
	if err != nil {
		t.Errorf("Error serializing for Pharos: %v", err)
		return
	}
	hash := make(map[string]interface{})
	err = json.Unmarshal(data, &hash)
	if err != nil {
		t.Errorf("Error unmarshalling data: %v", err)
	}

	assert.Equal(t, "uc.edu/cin.675812", hash["identifier"])
	assert.Equal(t, "cin.675812", hash["bag_name"])
	assert.Equal(t, "uc.edu", hash["institution"])
	assert.Equal(t, "Notes from the Oesper Collections", hash["title"])
	assert.Equal(t, "A collection from Cincinnati", hash["description"])
	assert.Equal(t, "Photo Collection", hash["alt_identifier"])
	assert.Equal(t, "institution", hash["access"])
}
