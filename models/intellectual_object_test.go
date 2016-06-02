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

func TestFindGenericFileByPath(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}

	gf1 := obj.FindGenericFileByPath("data/object.properties")
	assert.NotNil(t, gf1)
	assert.Equal(t, "uc.edu/cin.675812/data/object.properties", gf1.Identifier)

	gf2 := obj.FindGenericFileByPath("data/metadata.xml")
	assert.NotNil(t, gf2)
	assert.Equal(t, "uc.edu/cin.675812/data/metadata.xml", gf2.Identifier)

	// Make sure we don't get an error here
	assert.NotPanics(t, func() { obj.FindGenericFileByPath("file/does/not/exist") })
	gf3 := obj.FindGenericFileByPath("file/does/not/exist")
	assert.Nil(t, gf3)
}
