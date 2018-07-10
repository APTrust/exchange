package validation_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"strings"
	"testing"
)

func TestNewBagValidationConfig(t *testing.T) {
	conf := validation.NewBagValidationConfig()
	assert.NotNil(t, conf.FileSpecs)
	assert.NotNil(t, conf.TagSpecs)
	assert.False(t, conf.AllowMiscTopLevelFiles)
	assert.False(t, conf.AllowMiscDirectories)
	assert.False(t, conf.TopLevelDirMustMatchBagName)
}

func TestLoadBagValidationConfig(t *testing.T) {
	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		assert.Fail(t, errors[0].Error())
	}
	assert.True(t, conf.AllowMiscTopLevelFiles)
	assert.True(t, conf.AllowMiscDirectories)
	assert.True(t, conf.TopLevelDirMustMatchBagName)
	assert.Equal(t, 7, len(conf.FileSpecs))
	assert.Equal(t, 4, len(conf.TagSpecs))
	assert.Equal(t, 2, len(conf.FixityAlgorithms))

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
	if len(conf.FixityAlgorithms) > 1 {
		assert.Equal(t, "md5", conf.FixityAlgorithms[0])
		assert.Equal(t, "sha256", conf.FixityAlgorithms[1])
	}
	assert.Equal(t, validation.REQUIRED, conf.FileSpecs["manifest-md5.txt"].Presence)
	assert.Equal(t, validation.OPTIONAL, conf.FileSpecs["manifest-sha256.txt"].Presence)
	assert.Equal(t, "aptrust-info.txt", conf.TagSpecs["Title"].FilePath)
	assert.Equal(t, validation.REQUIRED, conf.TagSpecs["Title"].Presence)
	assert.False(t, conf.TagSpecs["Title"].EmptyOK)
	assert.Equal(t, 3, len(conf.TagSpecs["Access"].AllowedValues))
}

// Make sure we get an error and not a panic.
func TestLoadBagValidationConfigBadFiles(t *testing.T) {
	// Missing file
	configFilePath := path.Join("testdata", "json_objects", "file_does_not_exist.json")
	_, err := validation.LoadBagValidationConfig(configFilePath)
	assert.NotNil(t, err)
	// Unparseable JSON
	configFilePath = path.Join("testdata", "json_objects", "virginia.edu.uva-lib_2278801.tar")
	_, err = validation.LoadBagValidationConfig(configFilePath)
	assert.NotNil(t, err)
}

func TestValidPresenceValue(t *testing.T) {
	assert.True(t, validation.ValidPresenceValue("required"))
	assert.True(t, validation.ValidPresenceValue("optional"))
	assert.True(t, validation.ValidPresenceValue("forbidden"))
	assert.False(t, validation.ValidPresenceValue("naugahyde"))
}

func TestFileSpecValid(t *testing.T) {
	filespec := &validation.FileSpec{
		Presence: "required",
	}
	assert.True(t, filespec.Valid())
	filespec.Presence = "elastic"
	assert.False(t, filespec.Valid())
}

func TestTagSpecValid(t *testing.T) {
	tagspec := &validation.TagSpec{
		Presence: "verisimilitude",
	}
	assert.False(t, tagspec.Valid())
	tagspec.Presence = "optional"
	assert.False(t, tagspec.Valid())
	tagspec.FilePath = "data/blah/blah/blah.txt"
	assert.True(t, tagspec.Valid())
}

func TestValidateConfig(t *testing.T) {
	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		assert.Fail(t, errors[0].Error())
	}
	errors = conf.ValidateConfig()
	assert.Empty(t, errors)

	badPathSpec := validation.TagSpec{
		FilePath: "",
		Presence: "REQUIRED",
		EmptyOK:  true,
	}
	badPresenceSpec := validation.TagSpec{
		FilePath: "orangina",
		Presence: "orangina",
		EmptyOK:  true,
	}
	conf.TagSpecs["bad_path_spec"] = badPathSpec
	conf.TagSpecs["bad_presence"] = badPresenceSpec
	errors = conf.ValidateConfig()
	assert.Equal(t, 2, len(errors))
}

func TestCompileFileNameRegex(t *testing.T) {
	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		assert.Fail(t, errors[0].Error())
	}
	err := conf.CompileFileNameRegex()
	assert.Nil(t, err)

	conf.FileNamePattern = "ThisPatternIsInvalid[-"
	err = conf.CompileFileNameRegex()
	require.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Cannot compile regex"))

	conf.FileNamePattern = "aptrust"
	err = conf.CompileFileNameRegex()
	assert.Nil(t, err)
	assert.Equal(t, constants.APTrustFileNamePattern, conf.FileNameRegex)

	conf.FileNamePattern = "APTRUST"
	err = conf.CompileFileNameRegex()
	assert.Nil(t, err)
	assert.Equal(t, constants.APTrustFileNamePattern, conf.FileNameRegex)

	conf.FileNamePattern = "posix"
	err = conf.CompileFileNameRegex()
	assert.Nil(t, err)
	assert.Equal(t, constants.PosixFileNamePattern, conf.FileNameRegex)

	conf.FileNamePattern = "POSIX"
	err = conf.CompileFileNameRegex()
	assert.Nil(t, err)
	assert.Equal(t, constants.PosixFileNamePattern, conf.FileNameRegex)

}
