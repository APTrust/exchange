package workers_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
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
		assert.Fail(t, "Can't figure out Abs path: %v", err)
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "Error creating BagValidator: %v", err)
	}
	assert.NotNil(t, validator)
	assert.Equal(t, pathToBag, validator.PathToBag)
	assert.NotNil(t, validator.BagValidationConfig)
}

// Bad params should cause error, not panic.
func TestNewBagValidatorWithBadParams(t *testing.T) {
	// Good BagValidationConfig, bad bag path
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
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
		assert.Fail(t, "Can't figure out Abs path: %v", err)
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
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_good.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %v", err)
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
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.tagsample_bad.tar"))
	validator, err := workers.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %v", err)
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
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := workers.NewBagValidator(bagPath, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %v", err)
	}
	intelObj, readSummary := validator.ReadBag()
	assert.NotNil(t, intelObj)
	assert.Equal(t, 16, len(intelObj.GenericFiles))
	assert.Empty(t, intelObj.IngestErrorMessage)
	assert.False(t, readSummary.HasErrors())
}

// Read an invalid bag from a directory
func TestReadBag_FromDirectory_BagInvalid(t *testing.T) {

}

// Read from a file that does not exist.
func TestBagReadBag_FileDoesNotExist(t *testing.T) {

}

// Read from a file that is not a directory or a valid tar file.
func TestBagReadBag_BadFileFormat(t *testing.T) {

}

// A valid bag should have no errors.
func TestBagValidator_ValidBag(t *testing.T) {

}

// Make sure we catch all errors in an invalid bag.
func TestBagValidator_InvalidBag(t *testing.T) {

}
