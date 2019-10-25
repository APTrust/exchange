package validation_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/storage"
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

// These are some common errors that we expect to encounter repeatedly
// in our tests.

var err_0 = "File 'data/file-not-in-bag' in manifest 'manifest-sha256.txt' is missing from bag"
var err_1 = "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-md5.txt' is missing from bag"
var err_2 = "File 'custom_tags/tag_file_xyz.pdf' in manifest 'tagmanifest-sha256.txt' is missing from bag"
var err_3 = "Value for tag 'Title' is missing."
var err_4 = "Tag 'Access' has illegal value 'acksess'."
var err_5 = "Bad sha256 digest for 'data/datastream-descMetadata': manifest says 'This-checksum-is-bad-on-purpose.-The-validator-should-catch-it!!', file digest is 'cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7'"
var err_6 = "Bad md5 digest for 'custom_tags/tracked_tag_file.txt': manifest says '00000000000000000000000000000000', file digest is 'dafbffffc3ed28ef18363394935a2651'"
var err_7 = "Bad sha256 digest for 'custom_tags/tracked_tag_file.txt': manifest says '0000000000000000000000000000000000000000000000000000000000000000', file digest is '3f2f50c5bde87b58d6132faee14d1a295d115338643c658df7fa147e2296ccdd'"
var err_8 = "Tag 'Storage-Option' has illegal value 'cardboard-box'."

func getValidationConfig() (*validation.BagValidationConfig, error) {
	configFilePath := path.Join("testdata", "json_objects", "bag_validation_config.json")
	conf, errors := validation.LoadBagValidationConfig(configFilePath)
	if errors != nil && len(errors) > 0 {
		return nil, errors[0]
	}
	return conf, nil
}

func deleteFile(filePath string) {
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

func getValidator(t *testing.T, bagName string, includeExtendedMetadata bool) *validation.Validator {
	pathToBag := ""
	fullPath, _ := filepath.Abs(bagName)
	if bagName == fullPath {
		pathToBag = bagName
	} else {
		pathToBag = getBagPath(t, bagName)
	}
	bagValidationConfig := getConfig(t)
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, includeExtendedMetadata)
	if err != nil {
		assert.Fail(t, "Error creating Validator: %s", err.Error())
	}
	return validator
}

func validatorWithOptionalSpec(t *testing.T, bagName string) *validation.Validator {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %s", err.Error())
	}
	optionalFileSpec := validation.FileSpec{Presence: "OPTIONAL"}
	bagValidationConfig.FileSpecs["tagmanifest-md5.txt"] = optionalFileSpec
	pathToBag := getBagPath(t, bagName)
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	if err != nil {
		assert.Fail(t, "NewValidator returned unexpected error: %s", err.Error())
	}
	return validator
}

func TestNewValidator(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", true)
	defer deleteFile(validator.DBName())
	assert.NotNil(t, validator)
	assert.True(t, strings.HasSuffix(validator.PathToBag, "example.edu.tagsample_good.tar"))
	assert.NotNil(t, validator.BagValidationConfig)
	assert.True(t, validator.PreserveExtendedAttributes)
	assert.Equal(t, 0, validator.FileCount())
}

func TestNewValidator_BadConfig(t *testing.T) {
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
	pathToBag := getBagPath(t, "example.edu.tagsample_good.tar")
	_, err = validation.NewValidator(pathToBag, bagValidationConfig, true)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file ''"))
	assert.True(t, strings.Contains(err.Error(), "TagSpec for file 'orangina'"))
}

func TestNewValidator_BadRegex(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	bagValidationConfig.FileNamePattern = "ThisPatternIsInvalid[-"
	pathToBag := getBagPath(t, "example.edu.tagsample_good.tar")
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
		assert.Fail(t, "NewValidator should have raised error on non-existent file")
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
		assert.Fail(t, "NewValidator should have complained about bad bag path.")
	}
	pathToBag = getBagPath(t, "example.edu.tagsample_good.tar")
	_, err = validation.NewValidator(pathToBag, nil, true)
	if err == nil {
		assert.Fail(t, "NewValidator should have complained about nil BagValidationConfig.")
	}
}

// Read a valid bag from a tar file.
func TestValidator_FromTarFile_BagValid(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", true)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
	assert.Equal(t, 16, validator.FileCount())
}

// Read an invalid bag from a tar file.
func TestValidator_FromTarFile_BagInvalid(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_bad.tar", true)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())

	// Check for specific errors
	assert.True(t, util.StringListContains(summary.Errors, err_1))
	assert.True(t, util.StringListContains(summary.Errors, err_2))
	assert.True(t, util.StringListContains(summary.Errors, err_3))
	assert.True(t, util.StringListContains(summary.Errors, err_4))
	assert.True(t, util.StringListContains(summary.Errors, err_5))
	assert.True(t, util.StringListContains(summary.Errors, err_6))
	assert.True(t, util.StringListContains(summary.Errors, err_7))
	assert.True(t, util.StringListContains(summary.Errors, err_8))
	assert.Equal(t, 16, validator.FileCount())
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
	validator := getValidator(t, bagPath, true)
	defer deleteFile(validator.DBName())
	defer deleteFile(bagPath)
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
	assert.Equal(t, 16, validator.FileCount())
}

// Read an invalid bag from a directory, while tracking APTrust ingest data.
func TestValidator_FromDirectory_BagInvalid_NoMeta(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_bad.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	validator := getValidator(t, bagPath, true)
	defer deleteFile(validator.DBName())
	defer deleteFile(bagPath)
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	assert.Equal(t, 9, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, err_0))
	assert.True(t, util.StringListContains(summary.Errors, err_1))
	assert.True(t, util.StringListContains(summary.Errors, err_2))
	assert.True(t, util.StringListContains(summary.Errors, err_3))
	assert.True(t, util.StringListContains(summary.Errors, err_4))
	assert.True(t, util.StringListContains(summary.Errors, err_5))
	assert.True(t, util.StringListContains(summary.Errors, err_6))
	assert.True(t, util.StringListContains(summary.Errors, err_7))
	assert.Equal(t, 16, validator.FileCount())
}

// Read an invalid bag from a directory, without tracking APTrust
// ingest data.
func TestValidator_FromDirectory_BagInvalid(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_bad.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	validator := getValidator(t, bagPath, false)
	defer deleteFile(validator.DBName())
	defer deleteFile(bagPath)
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	assert.Equal(t, 9, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, err_0))
	assert.True(t, util.StringListContains(summary.Errors, err_1))
	assert.True(t, util.StringListContains(summary.Errors, err_2))
	assert.True(t, util.StringListContains(summary.Errors, err_3))
	assert.True(t, util.StringListContains(summary.Errors, err_4))
	assert.True(t, util.StringListContains(summary.Errors, err_5))
	assert.True(t, util.StringListContains(summary.Errors, err_6))
	assert.True(t, util.StringListContains(summary.Errors, err_7))
	assert.Equal(t, 16, validator.FileCount())
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
		assert.Fail(t, "NewValidator raised unexpected error: %s", err.Error())
	}
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.True(t, summary.HasErrors())
	assert.Equal(t, 10, len(summary.Errors))
	assert.True(t, strings.Contains(summary.Errors[0], "Error getting file iterator"))
	assert.True(t, strings.Contains(summary.Errors[1], "is not a directory"))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'tagmanifest-md5.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'bagit.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'bag-info.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'aptrust-info.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'manifest-md5.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Access' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Title' is missing."))
	assert.Equal(t, 0, validator.FileCount())
}

// Make sure we catch all errors in an invalid bag.
// This is a more thorough version of TestValidate_FromTarFile_BagInvalid
func TestValidator_InvalidBag(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_bad.tar", true)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	assert.Equal(t, 9, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, err_0))
	assert.True(t, util.StringListContains(summary.Errors, err_1))
	assert.True(t, util.StringListContains(summary.Errors, err_2))
	assert.True(t, util.StringListContains(summary.Errors, err_3))
	assert.True(t, util.StringListContains(summary.Errors, err_4))
	assert.True(t, util.StringListContains(summary.Errors, err_5))
	assert.True(t, util.StringListContains(summary.Errors, err_6))
	assert.True(t, util.StringListContains(summary.Errors, err_7))
	assert.Equal(t, 16, validator.FileCount())
}

// These good bags are from the old Bagman test suite. We have to make sure they
// pass here, so we know validation is backwards-compatible.
func TestValidator_GoodBags(t *testing.T) {
	goodBags := []string{
		"example.edu.multipart.b01.of02.tar",
		"example.edu.multipart.b02.of02.tar",
		"example.edu.sample_good.tar",
		"example.edu.sample_glacier_oh.tar",
		"example.edu.sample_glacier_or.tar",
		"example.edu.sample_glacier_va.tar",
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
			assert.Fail(t, "NewValidator returned unexpected error: %s", err.Error())
		}
		defer deleteFile(validator.DBName())
		summary, err := validator.Validate()
		require.Nil(t, err)
		assert.NotNil(t, summary)
		assert.False(t, summary.HasErrors())
	}
}

func TestValidator_BadAccess(t *testing.T) {
	validator := getValidator(t, "example.edu.sample_bad_access.tar", true)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 2, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'tagmanifest-md5.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Tag 'Access' has illegal value 'hands off!'."))
}

func TestValidator_BadChecksums(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_bad_checksums.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 5, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Access' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Bad md5 digest for 'data/datastream-DC': manifest says '44d85cf4810d6c6fe877BlahBlahBlah', file digest is '44d85cf4810d6c6fe87750117633e461'"))
	assert.True(t, util.StringListContains(summary.Errors, "Bad md5 digest for 'data/datastream-MARC': manifest says '93e381dfa9ad0086dbe3BlahBlahBlah', file digest is '93e381dfa9ad0086dbe3b92e0324bae6'"))
	assert.True(t, util.StringListContains(summary.Errors, "Bad md5 digest for 'data/datastream-RELS-EXT': manifest says 'ff731b9a1758618f6cc2BlahBlahBlah', file digest is 'ff731b9a1758618f6cc22538dede6174'"))
	assert.True(t, util.StringListContains(summary.Errors, "Bad md5 digest for 'data/datastream-descMetadata': manifest says '4bd0ad5f85c00ce84a45BlahBlahBlah', file digest is '4bd0ad5f85c00ce84a455466b24c8960'"))
}

func TestValidator_BadFileNames(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_bad_file_names.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 4, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Filename 'data/-starts-with-dash' is not valid according to APTrust validation rules"))
	assert.True(t, util.StringListContains(summary.Errors, "Filename 'data/contains#hash' is not valid according to APTrust validation rules"))
	assert.True(t, util.StringListContains(summary.Errors, "Filename 'data/contains*star' is not valid according to APTrust validation rules"))
	assert.True(t, util.StringListContains(summary.Errors, "Filename 'data/contains+plus' is not valid according to APTrust validation rules"))

}

func TestValidator_MissingDataFile(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_missing_data_file.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 2, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-DC' in manifest 'manifest-md5.txt' is missing from bag"))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Access' is missing."))
}

func TestValidator_NoAPTrustInfo(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_no_aptrust_info.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())

	assert.Equal(t, 3, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'aptrust-info.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Title' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Access' is missing."))
}

func TestValidator_NoBagInfo(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_no_bag_info.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 2, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'bag-info.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "Required tag 'Access' is missing."))
}

func TestValidator_NoDataDir(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_no_data_dir.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 4, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-DC' in manifest 'manifest-md5.txt' is missing from bag"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-descMetadata' in manifest 'manifest-md5.txt' is missing from bag"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-MARC' in manifest 'manifest-md5.txt' is missing from bag"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-RELS-EXT' in manifest 'manifest-md5.txt' is missing from bag"))
}

func TestValidator_NoMd5Manifest(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_no_md5_manifest.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 6, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Bag contains no payload manifest."))
	assert.True(t, util.StringListContains(summary.Errors, "Required file 'manifest-md5.txt' is missing."))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-DC' does not appear in any payload manifest (md5 or sha256)"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-MARC' does not appear in any payload manifest (md5 or sha256)"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-RELS-EXT' does not appear in any payload manifest (md5 or sha256)"))
	assert.True(t, util.StringListContains(summary.Errors, "File 'data/datastream-descMetadata' does not appear in any payload manifest (md5 or sha256)"))
}

func TestValidator_NoTitle(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_no_title.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 1, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Value for tag 'Title' is missing."))
}

func TestValidator_WrongFolderName(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_wrong_folder_name.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 1, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "Tarred bag should untar to directory 'example.edu.sample_wrong_folder_name', not 'wrong_folder_name'"))
}

func TestValidator_IllegalControlCharacter(t *testing.T) {
	validator := validatorWithOptionalSpec(t, "example.edu.sample_illegal_control.tar")
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	require.True(t, summary.HasErrors())
	assert.Equal(t, 1, len(summary.Errors))
	assert.True(t, util.StringListContains(summary.Errors, "File name 'data/datastream\\u007f.txt' contains an illegal unicode control character"))
}

var gfIdentifiers = []string{
	"example.edu.tagsample_good/aptrust-info.txt",
	"example.edu.tagsample_good/bag-info.txt",
	"example.edu.tagsample_good/bagit.txt",
	"example.edu.tagsample_good/custom_tag_file.txt",
	"example.edu.tagsample_good/junk_file.txt",
	"example.edu.tagsample_good/manifest-md5.txt",
	"example.edu.tagsample_good/manifest-sha256.txt",
	"example.edu.tagsample_good/tagmanifest-md5.txt",
	"example.edu.tagsample_good/tagmanifest-sha256.txt",
	"example.edu.tagsample_good/data/datastream-DC",
	"example.edu.tagsample_good/data/datastream-descMetadata",
	"example.edu.tagsample_good/data/datastream-MARC",
	"example.edu.tagsample_good/data/datastream-RELS-EXT",
	"example.edu.tagsample_good/custom_tags/tracked_file_custom.xml",
	"example.edu.tagsample_good/custom_tags/tracked_tag_file.txt",
	"example.edu.tagsample_good/custom_tags/untracked_tag_file.txt",
}

func TestValidator_SavesMinimumMetadata(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", false)
	defer deleteFile(validator.DBName())
	_, err := validator.Validate()
	require.Nil(t, err)
	boltDB, err := storage.NewBoltDB(validator.DBName())
	require.Nil(t, err)
	require.NotNil(t, boltDB)
	obj, err := boltDB.GetIntellectualObject("example.edu.tagsample_good")
	require.Nil(t, err)
	require.NotNil(t, obj)
	// Make sure we have basic obj info
	assert.NotEmpty(t, obj.Identifier)
	assert.NotEmpty(t, obj.Title)
	assert.NotEmpty(t, obj.Description)
	assert.NotEmpty(t, obj.Access)
	assert.NotEmpty(t, obj.AltIdentifier)
	assert.NotEmpty(t, obj.IngestTarFilePath)
	assert.NotEmpty(t, obj.IngestTopLevelDirNames)
	assert.Equal(t, "Charley Horse", obj.BagGroupIdentifier)
	assert.Equal(t, 10, len(obj.IngestTags))

	for _, identifier := range gfIdentifiers {
		gf, err := boltDB.GetGenericFile(identifier)
		require.Nil(t, err)
		require.NotNil(t, gf)
		// Make sure we have basic generic file info
		assert.NotEmpty(t, gf.Identifier)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier)
		assert.NotEmpty(t, gf.IngestMd5)
		assert.NotEmpty(t, gf.IngestSha256)
		assert.NotEmpty(t, gf.IngestFileType)
		// And we didn't bother to set info we're not interested in.
		assert.Empty(t, gf.Id)
		assert.Empty(t, gf.IntellectualObjectId)
		if strings.Contains(gf.Identifier, "manifest") {
			assert.Equal(t, "text/plain", gf.FileFormat)
		} else {
			assert.Empty(t, gf.FileFormat)
		}
		assert.Empty(t, gf.Size)
		assert.Empty(t, gf.FileCreated)
		assert.Empty(t, gf.FileModified)
		assert.Empty(t, gf.CreatedAt)
		assert.Empty(t, gf.UpdatedAt)
		assert.Nil(t, gf.Checksums)
		assert.Nil(t, gf.PremisEvents)
		assert.Empty(t, gf.LastFixityCheck)
		assert.Empty(t, gf.State)
		assert.Empty(t, gf.IngestMd5GeneratedAt)
		assert.NotEmpty(t, gf.IngestMd5VerifiedAt)
		assert.Empty(t, gf.IngestSha256GeneratedAt)
		assert.NotEmpty(t, gf.IngestSha256VerifiedAt)
		assert.Empty(t, gf.IngestUUIDGeneratedAt)
		assert.Empty(t, gf.IngestStoredAt)
		assert.Empty(t, gf.IngestReplicatedAt)
		assert.True(t, gf.IngestNeedsSave)
	}
}

func TestValidator_SavesExtendedMetadata(t *testing.T) {
	validator := getValidator(t, "example.edu.tagsample_good.tar", true)
	defer deleteFile(validator.DBName())
	_, err := validator.Validate()
	require.Nil(t, err)
	boltDB, err := storage.NewBoltDB(validator.DBName())
	require.Nil(t, err)
	require.NotNil(t, boltDB)
	obj, err := boltDB.GetIntellectualObject("example.edu.tagsample_good")
	require.Nil(t, err)
	require.NotNil(t, obj)
	// Make sure we have basic obj info
	assert.NotEmpty(t, obj.Identifier)
	assert.NotEmpty(t, obj.Title)
	assert.NotEmpty(t, obj.Description)
	assert.NotEmpty(t, obj.Access)
	assert.NotEmpty(t, obj.AltIdentifier)
	assert.NotEmpty(t, obj.IngestTarFilePath)
	assert.NotEmpty(t, obj.IngestTopLevelDirNames)
	assert.Equal(t, 10, len(obj.IngestTags))

	for _, identifier := range gfIdentifiers {
		gf, err := boltDB.GetGenericFile(identifier)
		require.Nil(t, err)
		require.NotNil(t, gf)
		// Make sure we have extended generic file info
		assert.NotEmpty(t, gf.Identifier)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier)
		assert.NotEmpty(t, gf.Size)
		assert.NotEmpty(t, gf.FileModified)
		assert.NotEmpty(t, gf.IngestFileType)
		assert.NotEmpty(t, gf.IngestMd5)
		assert.NotEmpty(t, gf.IngestMd5GeneratedAt)
		assert.NotEmpty(t, gf.IngestSha256)
		assert.NotEmpty(t, gf.IngestSha256GeneratedAt)
		assert.NotEmpty(t, gf.IngestFileType)
		assert.NotEmpty(t, gf.IngestUUID)
		assert.NotEmpty(t, gf.IngestUUIDGeneratedAt)
	}
}

func TestValidator_SetsStorageOption(t *testing.T) {
	goodBags := []string{
		"example.edu.multipart.b01.of02.tar",
		"example.edu.multipart.b02.of02.tar",
		"example.edu.sample_good.tar",
		"example.edu.sample_glacier_oh.tar",
		"example.edu.sample_glacier_or.tar",
		"example.edu.sample_glacier_va.tar",
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
			assert.Fail(t, "NewValidator returned unexpected error: %s", err.Error())
		}
		defer deleteFile(validator.DBName())
		summary, err := validator.Validate()
		require.Nil(t, err)
		assert.NotNil(t, summary)
		assert.False(t, summary.HasErrors())

		expectedOption := constants.StorageStandard
		if strings.Contains(goodBag, "glacier_oh") {
			expectedOption = constants.StorageGlacierOH
		} else if strings.Contains(goodBag, "glacier_or") {
			expectedOption = constants.StorageGlacierOR
		} else if strings.Contains(goodBag, "glacier_va") {
			expectedOption = constants.StorageGlacierVA
		}

		boltDB, err := storage.NewBoltDB(validator.DBName())
		require.Nil(t, err)
		require.NotNil(t, boltDB)
		obj, err := boltDB.GetIntellectualObject(validator.ObjIdentifier)
		require.Nil(t, err)
		require.NotNil(t, obj)
		assert.Equal(t, expectedOption, obj.StorageOption)

		for _, identifier := range boltDB.FileIdentifiers() {
			gf, err := boltDB.GetGenericFile(identifier)
			require.Nil(t, err)
			require.NotNil(t, gf)
			assert.Equal(t, expectedOption, gf.StorageOption)
		}
	}
}

// Bag has a fetch.txt file, and config says it's allowed
func TestNewValidator_LegalFetchTxt(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	bagValidationConfig.AllowFetchTxt = true
	pathToBag := getBagPath(t, "example.edu.fetchtxt.tar")
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	require.Nil(t, err)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.False(t, summary.HasErrors())
}

// Bag has a fetch.txt file, and config says it's NOT allowed
func TestNewValidator_IllegalFetchTxt(t *testing.T) {
	bagValidationConfig, err := getValidationConfig()
	if err != nil {
		assert.Fail(t, "Could not load BagValidationConfig: %v", err)
	}
	bagValidationConfig.AllowFetchTxt = false
	pathToBag := getBagPath(t, "example.edu.fetchtxt.tar")
	validator, err := validation.NewValidator(pathToBag, bagValidationConfig, true)
	require.Nil(t, err)
	defer deleteFile(validator.DBName())
	summary, err := validator.Validate()
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.True(t, summary.HasErrors())
	assert.True(t, util.StringListContains(summary.Errors, "Bag contains a fetch.txt file, but the profile does not allow it."))

}
