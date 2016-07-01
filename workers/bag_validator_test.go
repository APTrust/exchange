package workers_test

import (
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/models"
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
func TestValidate_FromTarFile_BagValid(t *testing.T) {
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
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 16, len(result.IntellectualObject.GenericFiles))
	assert.Empty(t, result.IntellectualObject.IngestErrorMessage)
	assert.False(t, result.ParseSummary.HasErrors())
}

// Read an invalid bag from a tar file.
func TestValidate_FromTarFile_BagInvalid(t *testing.T) {
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
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 16, len(result.IntellectualObject.GenericFiles))
	assert.NotEmpty(t, result.IntellectualObject.IngestErrorMessage)
	assert.True(t, result.ParseSummary.HasErrors())
}

// Read a valid bag from a directory
func TestValidate_FromDirectory_BagValid(t *testing.T) {
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
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 16, len(result.IntellectualObject.GenericFiles))
	assert.Empty(t, result.IntellectualObject.IngestErrorMessage)
	assert.False(t, result.ParseSummary.HasErrors())
}

// Read an invalid bag from a directory
func TestValidate_FromDirectory_BagInvalid(t *testing.T) {
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
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.NotEmpty(t, result.IntellectualObject.IngestErrorMessage)
	assert.True(t, result.ParseSummary.HasErrors())
}

// Read from a file that is not a directory or a valid tar file.
func TestValidate_BadFileFormat(t *testing.T) {
	_, thisfile, _, _ := runtime.Caller(0)
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := workers.NewBagValidator(thisfile, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator raised unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.True(t, result.ParseSummary.HasErrors())
	assert.NotEmpty(t, result.IntellectualObject.IngestErrorMessage)
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
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 16, len(result.IntellectualObject.GenericFiles))
	assert.Empty(t, result.IntellectualObject.IngestErrorMessage)
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.False(t, result.ValidationSummary.HasErrors())

	for _, gf := range result.IntellectualObject.GenericFiles {
		assert.NotEmpty(t, gf.IngestSha256VerifiedAt)
		assert.NotEmpty(t, gf.IngestMd5VerifiedAt)
	}
}

// Make sure we catch all errors in an invalid bag.
// This is a more thorough version of TestValidate_FromTarFile_BagInvalid
func TestValidate_InvalidBag(t *testing.T) {
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

	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 16, len(result.IntellectualObject.GenericFiles))
	assert.NotEmpty(t, result.IntellectualObject.IngestErrorMessage)
	assert.True(t, result.ParseSummary.HasErrors())
	assert.True(t, result.ValidationSummary.HasErrors())
	assert.True(t, result.HasErrors())


	err_0 := "File 'data/file-not-in-bag' in manifest 'manifest-sha256.txt' is missing from bag"
	err_1 := "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-md5.txt' is missing from bag"
	err_2 := "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-sha256.txt' is missing from bag"
	err_3 := "Value for tag 'Title' is missing."
	err_4 := "Tag 'Access' has illegal value 'acksess'."
	err_5 := "Bad sha256 digest for 'data/datastream-descMetadata': manifest says 'This-checksum-is-bad-on-purpose.-The-validator-should-catch-it!!', file digest is 'cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7'"
	err_6 := "Bad md5 digest for 'custom_tags/tracked_tag_file.txt': manifest says '00000000000000000000000000000000', file digest is 'dafbffffc3ed28ef18363394935a2651'"
	err_7 := "Bad sha256 digest for 'custom_tags/tracked_tag_file.txt': manifest says '0000000000000000000000000000000000000000000000000000000000000000', file digest is '3f2f50c5bde87b58d6132faee14d1a295d115338643c658df7fa147e2296ccdd'"
	assert.Equal(t, 8, len(result.ValidationSummary.Errors))
	assert.Equal(t, err_0, result.ValidationSummary.Errors[0])
	assert.Equal(t, err_1, result.ValidationSummary.Errors[1])
	assert.Equal(t, err_2, result.ValidationSummary.Errors[2])
	assert.Equal(t, err_3, result.ValidationSummary.Errors[3])
	assert.Equal(t, err_4, result.ValidationSummary.Errors[4])
	assert.Equal(t, err_5, result.ValidationSummary.Errors[5])
	assert.Equal(t, err_6, result.ValidationSummary.Errors[6])
	assert.Equal(t, err_7, result.ValidationSummary.Errors[7])
}

func getEmptyValidationResult() (*workers.ValidationResult) {
	return &workers.ValidationResult{
		ParseSummary: models.NewWorkSummary(),
		ValidationSummary: models.NewWorkSummary(),
		IntellectualObject: models.NewIntellectualObject(),
	}
}

func TestValidationResultHasErrors(t *testing.T) {
	result := getEmptyValidationResult()
	assert.False(t, result.HasErrors())

	result.ParseSummary.AddError("Oops!")
	assert.True(t, result.HasErrors())

	result = getEmptyValidationResult()
	result.ValidationSummary.AddError("Error")
	assert.True(t, result.HasErrors())

	result = getEmptyValidationResult()
	result.IntellectualObject.IngestErrorMessage = "My bad"
	assert.True(t, result.HasErrors())
}
