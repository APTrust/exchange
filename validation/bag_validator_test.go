package validation_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func getEmptyValidationResult() (*validation.ValidationResult) {
	return &validation.ValidationResult{
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

func getValidationConfig() (*validation.BagValidationConfig, error) {
	configFilePath := path.Join("testdata", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		return nil, errors[0]
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
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "Error creating BagValidator: %s", err.Error())
	}
	assert.NotNil(t, validator)
	assert.Equal(t, pathToBag, validator.PathToBag)
	assert.NotNil(t, validator.BagValidationConfig)
}

func TestNewBagValidator_BadConfig(t *testing.T) {
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
	badPathSpec := validation.TagSpec{
		FilePath: "",
		Presence: "REQUIRED",
		EmptyOK: true,
	}
	badPresenceSpec := validation.TagSpec{
		FilePath: "orangina",
		Presence: "orangina",
		EmptyOK: true,
	}
	bagValidationConfig.TagSpecs["bad_path_spec"] = badPathSpec
	bagValidationConfig.TagSpecs["bad_presence"] = badPresenceSpec
	_, err = validation.NewBagValidator(pathToBag, bagValidationConfig)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file ''"))
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file 'orangina'"))
}

func TestNewBagValidator_BadRegex(t *testing.T) {
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
	bagValidationConfig.FileNamePattern = "ThisPatternIsInvalid[-"
	_, err = validation.NewBagValidator(pathToBag, bagValidationConfig)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Cannot compile regex"))
}

// Validate a file that does not exist.
func TestNewBagValidator_FileDoesNotExist(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	_, err = validation.NewBagValidator("/blah/blah/blah", bagValidationConfig)
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
	_, err = validation.NewBagValidator(pathToBag, bagValidationConfig)
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
	_, err = validation.NewBagValidator(pathToBag, nil)
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
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(bagPath, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(bagPath, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(thisfile, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
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
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
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

	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_0))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_1))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_2))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_3))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_4))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_5))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_6))
	assert.True(t, util.StringListContains(result.ValidationSummary.Errors, err_7))
}

// ------------------------------------------------------------
// TODO: Test file name validation
// ------------------------------------------------------------

// ------------------------------------------------------------
// TODO: Test bad bags in testadata for specific errors
// ------------------------------------------------------------

// These good bags are from the old Bagman test suite. We have to make sure they
// pass here, so we know validation is backwards-compatible.
func TestValidate_GoodBags(t *testing.T) {
	goodBags := []string {
		"example.edu.multipart.b01.of02.tar",
		"example.edu.multipart.b02.of02.tar",
		"example.edu.sample_good.tar",
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	for _, goodBag := range goodBags {
		pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", goodBag))
		validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
		if err != nil {
			assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
		}
		result := validator.Validate()
		require.NotNil(t, result.IntellectualObject, goodBag)
		assert.NotEmpty(t, result.IntellectualObject.GenericFiles, goodBag)
		assert.Empty(t, result.IntellectualObject.IngestErrorMessage, goodBag)
		assert.False(t, result.ParseSummary.HasErrors(), goodBag)
	}
}

// ------------------------------------------------------------
// The bad bags below are from the old Bagman test suite.
// We have to make sure they fail for the same reasons they
// used to fail in Bagman, so we know validation is
// backwards-compatible.
// ------------------------------------------------------------

func TestValidate_BadAccess(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_bad_access.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 9, len(result.IntellectualObject.GenericFiles))
	assert.Equal(t, "Tag 'Access' has illegal value 'hands off!'.", result.IntellectualObject.IngestErrorMessage)
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_BadChecksums(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_bad_checksums.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 8, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Bad md5 digest for 'data/datastream-DC'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Bad md5 digest for 'data/datastream-descMetadata'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Bad md5 digest for 'data/datastream-MARC'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Bad md5 digest for 'data/datastream-RELS-EXT'"))
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_BadFileNames(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_bad_file_names.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 9, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Filename 'data/-starts-with-dash'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Filename 'data/contains#hash'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Filename 'data/contains*star'"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Filename 'data/contains+plus'"))
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_MissingDataFile(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_missing_data_file.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 7, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"File 'data/datastream-DC' in manifest 'manifest-md5.txt' is missing from bag"))
	assert.True(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_NoAPTrustInfo(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_no_aptrust_info.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 8, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Required file 'aptrust-info.txt' is missing."))
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_NoBagInfo(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_no_bag_info.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 7, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"Required file 'bag-info.txt' is missing."))
	assert.False(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
}

func TestValidate_NoDataDir(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{ Presence: "OPTIONAL" }
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "example.edu.sample_no_data_dir.tar"))
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	result := validator.Validate()
	assert.NotNil(t, result.IntellectualObject)
	assert.Equal(t, 5, len(result.IntellectualObject.GenericFiles))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"File 'data/datastream-DC' in manifest 'manifest-md5.txt' is missing"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"File 'data/datastream-descMetadata' in manifest 'manifest-md5.txt' is missing"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"File 'data/datastream-MARC' in manifest 'manifest-md5.txt' is missing"))
	assert.True(t, strings.Contains(result.IntellectualObject.IngestErrorMessage,
		"File 'data/datastream-RELS-EXT' in manifest 'manifest-md5.txt' is missing"))
	assert.True(t, result.ParseSummary.HasErrors())
	assert.NotNil(t, result.ValidationSummary)
	require.True(t, result.ValidationSummary.HasErrors())
	fmt.Println(result.IntellectualObject.IngestErrorMessage)
}
