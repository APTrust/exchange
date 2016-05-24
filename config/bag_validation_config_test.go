package config_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestNewBagValidationConfig(t *testing.T) {
	conf := config.NewBagValidationConfig()
	assert.NotNil(t, conf.FileSpecs)
	assert.NotNil(t, conf.TagSpecs)
	assert.False(t, conf.AllowMiscTopLevelFiles)
	assert.False(t, conf.AllowMiscDirectories)
	assert.False(t, conf.TopLevelDirMustMatchBagName)
}

func TestLoadBagValidationConfig(t *testing.T) {
	configFilePath := path.Join("testdata", "bag_validation_config.json")
	conf, err := config.LoadBagValidationConfig(configFilePath)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.True(t, conf.AllowMiscTopLevelFiles)
	assert.True(t, conf.AllowMiscDirectories)
	assert.True(t, conf.TopLevelDirMustMatchBagName)
	assert.Equal(t, 6, len(conf.FileSpecs))
	assert.Equal(t, 3, len(conf.TagSpecs))

	// Spot checks
	if _, ok := conf.FileSpecs["manifest-md5.txt"]; !ok {
		assert.Fail(t, "FileSpec for manifest-md5.txt is missing")
	}
	if _, ok := conf.FileSpecs["manifest-sha256.txt"]; !ok {
		assert.Fail(t, "FileSpec for manifest-sha256.txt is missing")
	}
	if _, ok := conf.TagSpecs["Title"]; !ok {
		assert.Fail(t, "TagSpec for Title is missing")
	}
	assert.Equal(t, config.REQUIRED, conf.FileSpecs["manifest-md5.txt"].Presence)
	assert.Equal(t, config.OPTIONAL, conf.FileSpecs["manifest-sha256.txt"].Presence)
	assert.Equal(t, "aptrust-info.txt", conf.TagSpecs["Title"].FilePath)
	assert.Equal(t, config.REQUIRED, conf.TagSpecs["Title"].Presence)
	assert.False(t, conf.TagSpecs["Title"].EmptyOK)
}

// Make sure we get an error and not a panic.
func TestLoadBagValidationConfigBadFiles(t *testing.T) {
	// Missing file
	configFilePath := path.Join("testdata", "file_does_not_exist.json")
	_, err := config.LoadBagValidationConfig(configFilePath)
	assert.NotNil(t, err)
	// Unparseable JSON
	configFilePath = path.Join("testdata", "virginia.edu.uva-lib_2278801.tar")
	_, err = config.LoadBagValidationConfig(configFilePath)
	assert.NotNil(t, err)
}

func TestValidPresenceValue(t *testing.T) {
	assert.True(t, config.ValidPresenceValue("required"))
	assert.True(t, config.ValidPresenceValue("optional"))
	assert.True(t, config.ValidPresenceValue("forbidden"))
	assert.False(t, config.ValidPresenceValue("naugahyde"))
}

func TestFileSpecValid(t *testing.T) {
	filespec := &config.FileSpec{
		Presence: "required",
	}
	assert.True(t, filespec.Valid())
	filespec.Presence = "elastic"
	assert.False(t, filespec.Valid())
}

func TestTagSpecValid(t *testing.T) {
	tagspec := &config.TagSpec{
		Presence: "verisimilitude",
	}
	assert.False(t, tagspec.Valid())
	tagspec.Presence = "optional"
	assert.False(t, tagspec.Valid())
	tagspec.FilePath = "data/blah/blah/blah.txt"
	assert.True(t, tagspec.Valid())
}
