package config_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestNewBagValidationConfig(t *testing.T) {
	config := config.NewBagValidationConfig()
	assert.NotNil(t, config.FileSpecs)
	assert.NotNil(t, config.TagSpecs)
	assert.False(t, config.AllowMiscTopLevelFiles)
	assert.False(t, config.AllowMiscDirectories)
	assert.False(t, config.TopLevelDirMustMatchBagName)
}

func TestLoadBagValidationConfig(t *testing.T) {
	configFilePath := path.Join("testdata", "bag_validation_config.json")
	config, err := config.LoadBagValidationConfig(configFilePath)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.True(t, config.AllowMiscTopLevelFiles)
	assert.True(t, config.AllowMiscDirectories)
	assert.True(t, config.TopLevelDirMustMatchBagName)

}

func TestLoadBagValidationConfigBadFiles(t *testing.T) {

}
