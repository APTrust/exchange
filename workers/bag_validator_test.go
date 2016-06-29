package workers_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"path"
	"path/filepath"
	"runtime"
	"testing"
)

func getContext() (*context.Context, error) {
	configFile := filepath.Join("testdata", "config.json")
	appConfig, err := config.Load(configFile, "test")
	if err != nil {
		return nil, err
	}
	appConfig.LogToStderr = false
	return context.NewContext(appConfig), nil
}

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
	_context, err := getContext()
	if err != nil {
		assert.Fail(t, "Could not create Context object: %v", err)
	}
	validator, err := workers.NewBagValidator(_context, pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "Error creating BagValidator: %v", err)
	}
	assert.NotNil(t, validator)
	assert.Equal(t, pathToBag, validator.PathToBag)
	assert.NotNil(t, validator.BagValidationConfig)
}

// Bad params should cause error, not panic.
func TestNewBagValidatorWithBadParams(t *testing.T) {
	_context, err := getContext()
	if err != nil {
		assert.Fail(t, "Could not create Context object: %v", err)
	}

	// Good BagValidationConfig, bad bag path
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	pathToBag := "/path/does/not/exist.tar"
	_, err = workers.NewBagValidator(_context, pathToBag, bagValidationConfig)
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
	_, err = workers.NewBagValidator(_context, pathToBag, nil)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about nil BagValidationConfig.")
	}

	// No Context
	_, err = workers.NewBagValidator(nil, pathToBag, bagValidationConfig)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about nil Context.")
	}
}

// Read a valid bag from a tar file.
func TestReadBag_FromTarFile_BagValid(t *testing.T) {

}

// Read an invalid bag from a tar file.
func TestReadBag_FromTarFile_BagInvalid(t *testing.T) {

}

// Read a valid bag from a directory
func TestReadBag_FromDirectory_BagValid(t *testing.T) {

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
