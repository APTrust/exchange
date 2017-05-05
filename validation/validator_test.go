package validation_test

import (
	"fmt"
	//	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
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

// func getValidationConfig() (*validation.BagValidationConfig, error) {
// 	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
// 	conf, errors := validation.LoadBagValidationConfig(configFilePath)
// 	if errors != nil && len(errors) > 0 {
// 		return nil, errors[0]
// 	}
// 	return conf, nil
// }

func deleteDBFile(filePath string) {
	if fileutil.LooksSafeToDelete(filePath, 12, 3) {
		os.Remove(filePath)
	}
}

func getBagPath(t *testing.T, bagname string) string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", bagname))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	return pathToBag
}

func getConfig(t *testing.T) *validation.BagValidationConfig {
	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		assert.Fail(t, "Could not load BagValidationConfig: %v", errors[0])
	}
	return conf
}

func getValidator(t *testing.T, bagName string, includeExtendedMetada bool) *validation.Validator {
	pathToBag := getBagPath(t, bagName)
	bagValidationConfig := getConfig(t)
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "Error creating BagValidator: %s", err.Error())
	}
	return validator
}

func TestNewValidator(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", true)
	defer deleteDBFile(validator.DBName())
	assert.NotNil(t, validator)
	assert.True(t, strings.HasSuffix(validator.PathToBag, "example.edu.tagsample_good.tar"))
	assert.NotNil(t, validator.BagValidationConfig)
	assert.True(t, validator.PreserveExtendedAttributes)
}

func TestNewValidator_BadConfig(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_good.tar"))
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
		EmptyOK:  true,
	}
	badPresenceSpec := validation.TagSpec{
		FilePath: "orangina",
		Presence: "orangina",
		EmptyOK:  true,
	}
	bagValidationConfig.TagSpecs["bad_path_spec"] = badPathSpec
	bagValidationConfig.TagSpecs["bad_presence"] = badPresenceSpec
	_, err = validation.NewValidator(pathToBag, bagValidationConfig, true)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file ''"))
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file 'orangina'"))
}

func TestNewValidator_BadRegex(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_good.tar"))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	bagValidationConfig.FileNamePattern = "ThisPatternIsInvalid[-"
	_, err = validation.NewValidator(pathToBag, bagValidationConfig, true)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Cannot compile regex"))
}

// Validate a file that does not exist.
func TestNewValidator_FileDoesNotExist(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	_, err = validation.NewValidator("/blah/blah/blah", bagValidationConfig, true)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have raised error on non-existent file")
	}
}

// Bad params should cause error, not panic.
func TestNewValidatorWithBadParams(t *testing.T) {
	// Good BagValidationConfig, bad bag path
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	pathToBag := "/path/does/not/exist.tar"
	_, err = validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about bad bag path.")
	}

	// Good bag path, bad BagValidationConfig
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err = filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_good.tar"))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	_, err = validation.NewValidator(pathToBag, nil, true)
	if err == nil {
		assert.Fail(t, "NewBagValidator should have complained about nil BagValidationConfig.")
	}
}

// Read a valid bag from a tar file.
func TestValidator_FromTarFile_BagValid(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", true)
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
	fmt.Println(summary.AllErrorsAsString())
}

// Read an invalid bag from a tar file.
func TestValidator_FromTarFile_BagInvalid(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_bad.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	// TODO: Check for specific errors
}

// Read a valid bag from a directory
func TestValidator_FromDirectory_BagValid(t *testing.T) {
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
	validator, err := validation.NewValidator(bagPath, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
	fmt.Println(summary.AllErrorsAsString())
}

// Read an invalid bag from a directory
func TestValidator_FromDirectory_BagInvalid(t *testing.T) {
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
	validator, err := validation.NewValidator(bagPath, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

// Read from a file that is not a directory or a valid tar file.
func TestValidator_BadFileFormat(t *testing.T) {
	_, thisfile, _, _ := runtime.Caller(0)
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	validator, err := validation.NewValidator(thisfile, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator raised unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

// A valid bag should have no errors.
func TestValidator_ValidBag(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_good.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
	fmt.Println(summary.AllErrorsAsString())
}

// Make sure we catch all errors in an invalid bag.
// This is a more thorough version of TestValidate_FromTarFile_BagInvalid
func TestValidator_InvalidBag(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.tagsample_bad.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())

	err_0 := "File 'data/file-not-in-bag' in manifest 'manifest-sha256.txt' is missing from bag"
	err_1 := "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-md5.txt' is missing from bag"
	err_2 := "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-sha256.txt' is missing from bag"
	err_3 := "Value for tag 'Title' is missing."
	err_4 := "Tag 'Access' has illegal value 'acksess'."
	err_5 := "Bad sha256 digest for 'data/datastream-descMetadata': manifest says 'This-checksum-is-bad-on-purpose.-The-validator-should-catch-it!!', file digest is 'cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7'"
	err_6 := "Bad md5 digest for 'custom_tags/tracked_tag_file.txt': manifest says '00000000000000000000000000000000', file digest is 'dafbffffc3ed28ef18363394935a2651'"
	err_7 := "Bad sha256 digest for 'custom_tags/tracked_tag_file.txt': manifest says '0000000000000000000000000000000000000000000000000000000000000000', file digest is '3f2f50c5bde87b58d6132faee14d1a295d115338643c658df7fa147e2296ccdd'"
	assert.Equal(t, 8, len(summary.Errors))

	fmt.Println(summary.AllErrorsAsString())

	assert.True(t, util.StringListContains(summary.Errors, err_0))
	assert.True(t, util.StringListContains(summary.Errors, err_1))
	assert.True(t, util.StringListContains(summary.Errors, err_2))
	assert.True(t, util.StringListContains(summary.Errors, err_3))
	assert.True(t, util.StringListContains(summary.Errors, err_4))
	assert.True(t, util.StringListContains(summary.Errors, err_5))
	assert.True(t, util.StringListContains(summary.Errors, err_6))
	assert.True(t, util.StringListContains(summary.Errors, err_7))
}

// These good bags are from the old Bagman test suite. We have to make sure they
// pass here, so we know validation is backwards-compatible.
func TestValidator_GoodBags(t *testing.T) {
	goodBags := []string{
		"example.edu.multipart.b01.of02.tar",
		"example.edu.multipart.b02.of02.tar",
		"example.edu.sample_good.tar",
	}
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	for _, goodBag := range goodBags {
		pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", goodBag))
		validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
		if err != nil {
			assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
		}
		defer deleteDBFile(validator.DBName())
		summary, err := validator.Validate()
		require.Nil(t, err)
		assert.NotNil(t, summary)
		assert.False(t, summary.HasErrors())
	}
}

// ------------------------------------------------------------
// The bad bags below are from the old Bagman test suite.
// We have to make sure they fail for the same reasons they
// used to fail in Bagman, so we know validation is
// backwards-compatible.
// ------------------------------------------------------------

func TestValidator_BadAccess(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_bad_access.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_BadChecksums(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_bad_checksums.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_BadFileNames(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_bad_file_names.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())

	fmt.Println(summary.AllErrorsAsString())
	// TODO: Check specific errors
}

func TestValidator_MissingDataFile(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_missing_data_file.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_NoAPTrustInfo(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_no_aptrust_info.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_NoBagInfo(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_no_bag_info.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_NoDataDir(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_no_data_dir.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())

	fmt.Println(summary.AllErrorsAsString())
	// TODO: Check specific errors
}

func TestValidator_NoMd5Manifest(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_no_md5_manifest.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_NoTitle(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_no_title.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}

func TestValidator_WrongFolderName(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "testdata", "unit_test_bags", "example.edu.sample_wrong_folder_name.tar"))
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewBagValidator returned unexpected error: %s", err.Error())
	}
	defer deleteDBFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	// TODO: Check specific errors
}
