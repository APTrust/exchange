package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	filepath := filepath.Join("testdata", "json_objects", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}

func TestSerializeObjectForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
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

func TestFindGenericFile(t *testing.T) {
	filepath := filepath.Join("testdata", "json_objects", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}

	gf1 := obj.FindGenericFile("data/object.properties")
	assert.NotNil(t, gf1)
	assert.Equal(t, "uc.edu/cin.675812/data/object.properties", gf1.Identifier)

	gf2 := obj.FindGenericFile("data/metadata.xml")
	assert.NotNil(t, gf2)
	assert.Equal(t, "uc.edu/cin.675812/data/metadata.xml", gf2.Identifier)

	// Make sure we don't get an error here
	assert.NotPanics(t, func() { obj.FindGenericFile("file/does/not/exist") })
	gf3 := obj.FindGenericFile("file/does/not/exist")
	assert.Nil(t, gf3)
}

func TestFindTag(t *testing.T) {
	obj := models.NewIntellectualObject()
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file1", "label1", "value1"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file2", "label2", "value2"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file3", "label3", "value3.0"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file3", "label3", "value3.1"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file4", "label3", "value3.2"))


	tags1 := obj.FindTag("label1")
	tags2 := obj.FindTag("label2")
	tags3 := obj.FindTag("label3")
	tagsx := obj.FindTag("Elmer Fudd")

	require.NotNil(t, tags1)
	assert.Equal(t, "value1", tags1[0].Value)
	require.NotNil(t, tags2)
	assert.Equal(t, "value2", tags2[0].Value)
	require.NotNil(t, tags3)
	assert.Equal(t, 3, len(tags3))
	assert.Equal(t, "value3.0", tags3[0].Value)
	assert.Equal(t, "value3.1", tags3[1].Value)
	assert.Equal(t, "value3.2", tags3[2].Value)
	assert.Nil(t, tagsx)
}
