package workers_test

import (
	"fmt"
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
)

func getValidationConfig() (*config.BagValidationConfig, error) {
	configFilePath := path.Join("testdata", "bag_validation_config.json")
	conf, err := config.LoadBagValidationConfig(configFilePath)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func TestNewBagValidator(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_good.tar"))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "Error creating BagValidator: %s", err.Error())
	}
	assert.NotNil(t, validator)
	assert.Equal(t, pathToBag, validator.PathToBag)
	assert.NotNil(t, validator.BagValidationConfig)
}

// Validate a file that does not exist.
func TestNewBagValidator_FileDoesNotExist(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	_, err = workers.NewBagValidator("/blah/blah/blah", bagValidationConfig)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have raised error on non-existent file")
	}
}

// Bad params should cause error, not panic.
func TestNewBagValidatorWithBadParams(t *testing.T) {
	// Good BagValidationConfig, bad bag path
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	pathToBag := "/path/does/not/exist.tar"
	_, err = workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about bad bag path.")
	}

	// Good bag path, bad BagValidationConfig
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err = filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_good.tar"))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	_, err = workers.NewBagValidator(pathToBag, nil)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about nil BagValidationConfig.")
	}
}

// Read a valid bag from a tar file.
func TestReadBag_FromTarFile_BagValid(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_good.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.Empty(t, intelObj.IngestErrorMessage)
	assert.False(t, readSummary.HasErrors())
}

// Read an invalid bag from a tar file.
func TestReadBag_FromTarFile_BagInvalid(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_bad.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.NotEmpty(t, intelObj.IngestErrorMessage)
	assert.True(t, readSummary.HasErrors())
}

// Read a valid bag from a directory
func TestReadBag_FromDirectory_BagValid(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_good.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	validator, err := workers.NewBagValidator(bagPath, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.Empty(t, intelObj.IngestErrorMessage)
	assert.False(t, readSummary.HasErrors())
}

// Read an invalid bag from a directory
func TestReadBag_FromDirectory_BagInvalid(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_bad.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	validator, err := workers.NewBagValidator(bagPath, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.NotEmpty(t, intelObj.IngestErrorMessage)
	assert.True(t, readSummary.HasErrors())
}

// Read from a file that is not a directory or a valid tar file.
func TestReadBag_BadFileFormat(t *testing.T) {
	_, thisfile, _, _ := runtime.Caller(0)
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := workers.NewBagValidator(thisfile, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator raised unexpected error: %s", err.Error())
	}
	intelObj, vbagSummary := validator.ReadBag()
	assert.True(t, vbagSummary.HasErrors())
	assert.NotEmpty(t, intelObj.IngestErrorMessage)
}

// A valid bag should have no errors.
func TestBagValidator_ValidBag(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_good.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.Empty(t, intelObj.IngestErrorMessage)
	assert.False(t, readSummary.HasErrors())

	validationSummary := validator.Validate()
	assert.NotNil(t, validationSummary)
	require.False(t, validationSummary.HasErrors())
}

// Make sure we catch all errors in an invalid bag.
// This is a more thorough version of TestReadBag_FromTarFile_BagInvalid
func TestBagValidator_InvalidBag(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_bad.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}

	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.NotEmpty(t, intelObj.IngestErrorMessage)
	assert.True(t, readSummary.HasErrors())

	validationSummary := validator.Validate()

	// ---------------------------------
	// TODO: Assert all specific errors were caught, and get rid of print statement.
	// ---------------------------------
	fmt.Println(validationSummary.AllErrorsAsString())
	fmt.Println("-------------------------------------")
	fmt.Println(intelObj.IngestErrorMessage)
}
