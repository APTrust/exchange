package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/testhelper"
	"github.com/APTrust/exchange/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestNewVirtualBag(t *testing.T) {
	tarFilePath := testhelper.VbagGetPath("example.edu.tagsample_good.tar")
	vbag := models.NewVirtualBag(tarFilePath, nil, false, false)
	assert.NotNil(t, vbag)
}

func TestVirtualBagRead_FromDirectory(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_good.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(bagPath, files, true, true)
	assert.NotNil(t, vbag)
	obj, summary := vbag.Read()
	runAssertions(t, obj, summary, "TestVirtualBagRead_FromDirectory")
}

func TestVirtualBagRead_FromTarFile(t *testing.T) {
	tarFilePath := testhelper.VbagGetPath("example.edu.tagsample_good.tar")
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, summary := vbag.Read()
	runAssertions(t, obj, summary, "TestVirtualBagRead_FromTarFile")
}

func TestVirtualBagRead_ChecksumOptions(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_good.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(bagPath, files, true, false)
	assert.NotNil(t, vbag)
	obj, _ := vbag.Read()

	// Should calculate md5 only
	for _, gf := range obj.GenericFiles {
		assert.NotEmpty(t, gf.IngestMd5)
		assert.Empty(t, gf.IngestSha256)
	}

	vbag = models.NewVirtualBag(bagPath, files, false, true)
	assert.NotNil(t, vbag)
	obj, _ = vbag.Read()

	// Should calculate sha256 only
	for _, gf := range obj.GenericFiles {
		assert.Empty(t, gf.IngestMd5)
		assert.NotEmpty(t, gf.IngestSha256)
	}

}

// With md5 manifest only, sha256 only, and both
func TestVirtualBagRead_ManifestOptions(t *testing.T) {
	tempDir, bagPath, err := testhelper.UntarTestBag("example.edu.tagsample_good.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}

	// Delete the md5 manifest
	os.Remove(filepath.Join(bagPath, "manifest-md5.txt"))

	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(bagPath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ := vbag.Read()

	// Should have manifest values for sha256
	for _, gf := range obj.GenericFiles {
		if gf.IngestFileType == constants.PAYLOAD_FILE {
			assert.Empty(t, gf.IngestManifestMd5)
			assert.NotEmpty(t, gf.IngestManifestSha256)
		}
	}

	tempDir, bagPath, err = testhelper.UntarTestBag("example.edu.tagsample_good.tar")
	if err != nil {
		assert.Fail(t, err.Error())
	}
	if tempDir != "" {
		defer os.RemoveAll(tempDir)
	}

	// Delete the sha256 manifest
	os.Remove(filepath.Join(bagPath, "manifest-sha256.txt"))

	files = []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag = models.NewVirtualBag(bagPath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ = vbag.Read()

	// Should have manifest values for sha256
	for _, gf := range obj.GenericFiles {
		if gf.IngestFileType == constants.PAYLOAD_FILE {
			assert.NotEmpty(t, gf.IngestManifestMd5)
			assert.Empty(t, gf.IngestManifestSha256)
		}
	}
}

// We should parse the tags in the specified files.
// We should not parse other tag files
func TestVirtualBagTagFileOptions(t *testing.T) {
	tarFilePath := testhelper.VbagGetPath("example.edu.tagsample_good.tar")
	files := []string {}
	vbag := models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ := vbag.Read()
	assert.Equal(t, 0, len(obj.IngestTags))

	files = []string {"bagit.txt"}
	vbag = models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ = vbag.Read()
	assert.Equal(t, 2, len(obj.IngestTags))

	files = []string {"bagit.txt", "bag-info.txt"}
	vbag = models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ = vbag.Read()
	assert.Equal(t, 8, len(obj.IngestTags))

	files = []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag = models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, _ = vbag.Read()
	assert.Equal(t, 10, len(obj.IngestTags))
}

func TestVirtualBagReadReportsMissingFiles(t *testing.T) {
	// This bad bag has a number of problems.
	// Here, we're specifically testing to see if a missing file is reported.
	tarFilePath := testhelper.VbagGetPath("example.edu.tagsample_bad.tar")
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, summary := vbag.Read()

	// custom_tags/tag_file_xyz.pdf appears twice in missing files
	// list because it's mentioned in manifest-md5 and manifest-sha256
	assert.True(t, summary.HasErrors())
	require.Equal(t, 3, len(obj.IngestMissingFiles))
	assert.Equal(t, "data/file-not-in-bag", obj.IngestMissingFiles[0].FilePath)
	assert.Equal(t, "custom_tags/tag_file_xyz.pdf", obj.IngestMissingFiles[1].FilePath)
	assert.Equal(t, "custom_tags/tag_file_xyz.pdf", obj.IngestMissingFiles[2].FilePath)
}


func runAssertions(t *testing.T, obj *models.IntellectualObject, summary *models.WorkSummary, caller string) {
	// WorkSummary
	assert.False(t, summary.StartedAt.IsZero(), caller)
	assert.False(t, summary.FinishedAt.IsZero(), caller)
	assert.Empty(t, summary.Errors, caller)

	// IntelObj properties
	assert.Equal(t, 0, obj.Id, caller)
	assert.Equal(t, "example.edu.tagsample_good", obj.Identifier, caller)

	// TODO: Is BagName necessary? It should be the same as obj.Identifier
	assert.Equal(t, "", obj.BagName, caller)

	assert.Equal(t, "virginia.edu", obj.Institution, caller)
	assert.Equal(t, 0, obj.InstitutionId, caller)
	assert.Equal(t, "Thirteen Ways of Looking at a Blackbird", obj.Title, caller)
	assert.Equal(t, "so much depends upon a red wheel barrow glazed with rain water beside the white chickens", obj.Description, caller)
	assert.Equal(t, "Institution", obj.Access, caller)
	assert.Equal(t, "uva-internal-id-0001", obj.AltIdentifier, caller)
	assert.Empty(t, obj.IngestErrorMessage, caller)
	if caller == "TestVirtualBagRead_FromTarFile" {
		assert.NotEmpty(t, obj.IngestTarFilePath, caller)
	} else if caller == "TestVirtualBagRead_FromDirectory" {
		assert.NotEmpty(t, obj.IngestUntarredPath, caller)
	}
	assert.Equal(t, 2, len(obj.IngestManifests), caller)
	assert.True(t, util.StringListContains(obj.IngestManifests, "manifest-md5.txt"), caller)
	assert.True(t, util.StringListContains(obj.IngestManifests, "manifest-sha256.txt"), caller)
	assert.Equal(t, 2, len(obj.IngestTagManifests), caller)
	assert.True(t, util.StringListContains(obj.IngestTagManifests, "tagmanifest-md5.txt"), caller)
	assert.True(t, util.StringListContains(obj.IngestTagManifests, "tagmanifest-sha256.txt"), caller)
	assert.Empty(t, obj.IngestFilesIgnored, caller)

	// Tags
	assert.Equal(t, 10, len(obj.IngestTags))
	for _, tag := range obj.IngestTags {
		assert.NotEmpty(t, tag.SourceFile, caller)
		assert.NotEmpty(t, tag.Label, caller)
		assert.NotEmpty(t, tag.Value, caller)
	}

	// Spot check one tag
	tag := obj.IngestTags[4]
	assert.Equal(t, "bag-info.txt", tag.SourceFile, caller)
	assert.Equal(t, "Bag-Count", tag.Label, caller)
	assert.Equal(t, "1 of 1", tag.Value, caller)

	// Generic Files
	tagFileCount := 0
	payloadFileCount := 0
	manifestCount := 0
	tagManifestCount := 0
	assert.Equal(t, 16, len(obj.GenericFiles), caller)
	for _, gf := range obj.GenericFiles {
		assert.NotEmpty(t, gf.Identifier, caller)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier, caller)
		assert.NotEmpty(t, gf.FileFormat, caller)
		assert.NotEmpty(t, gf.IngestFileType, caller)
		assert.NotEmpty(t, gf.IngestMd5, caller)
		assert.NotEmpty(t, gf.IngestSha256, caller)

		if caller == "TestVirtualBagRead_FromTarFile" {
			assert.Empty(t, gf.IngestLocalPath, caller)
		} else if caller == "TestVirtualBagRead_FromDirectory" {
			assert.NotEmpty(t, gf.IngestLocalPath, caller)
		}

		assert.Empty(t, gf.IngestStorageURL, caller)
		assert.Empty(t, gf.IngestReplicationURL, caller)
		assert.True(t, gf.Size > 0, caller)
		switch gf.IngestFileType {
		case constants.PAYLOAD_FILE: payloadFileCount++
		case constants.PAYLOAD_MANIFEST: manifestCount++
		case constants.TAG_MANIFEST: tagManifestCount++
		case constants.TAG_FILE: tagFileCount++
		}
	}

	// Make sure file types were all tagged correctly
	assert.Equal(t, 4, payloadFileCount, caller)
	assert.Equal(t, 2, manifestCount, caller)
	assert.Equal(t, 2, tagManifestCount, caller)
	assert.Equal(t, 8, tagFileCount, caller)

	// Spot check generic file aptrust-info.txt
	gf := obj.FindGenericFile("aptrust-info.txt")
	if gf == nil {
		assert.Fail(t, "Could not find aptrust-info.txt", caller)
	}
	assert.Equal(t, "example.edu.tagsample_good/aptrust-info.txt", gf.Identifier, caller)
	assert.Equal(t, 0, gf.IntellectualObjectId, caller)
	assert.Equal(t, "example.edu.tagsample_good", gf.IntellectualObjectIdentifier, caller)
	assert.Equal(t, "application/binary", gf.FileFormat, caller)
	assert.Empty(t, gf.URI, caller)
	assert.EqualValues(t, 67, gf.Size, caller)
	assert.False(t, gf.FileModified.IsZero(), caller)
	assert.Equal(t, constants.TAG_FILE, gf.IngestFileType, caller)
	assert.Equal(t, "300e936e622605f9f7a846d261d53093", gf.IngestManifestMd5, caller)
	assert.Equal(t, "300e936e622605f9f7a846d261d53093", gf.IngestMd5, caller)
	assert.False(t, gf.IngestMd5GeneratedAt.IsZero(), caller)
	assert.True(t, gf.IngestMd5VerifiedAt.IsZero(), caller)
	assert.Equal(t, "a2b6c5a713af771c5e4edde8d5be25fbcad86e45ea338f43a5bb769347e7c8bb", gf.IngestManifestSha256, caller)
	assert.Equal(t, "a2b6c5a713af771c5e4edde8d5be25fbcad86e45ea338f43a5bb769347e7c8bb", gf.IngestSha256, caller)
	assert.False(t, gf.IngestSha256GeneratedAt.IsZero(), caller)
	assert.True(t, gf.IngestSha256VerifiedAt.IsZero(), caller)
	assert.NotEmpty(t, gf.IngestUUID, caller)
	assert.False(t, gf.IngestUUIDGeneratedAt.IsZero(), caller)
	assert.Empty(t, gf.IngestStorageURL, caller)
	assert.True(t, gf.IngestStoredAt.IsZero(), caller)
	assert.Empty(t, gf.IngestReplicationURL, caller)
	assert.True(t, gf.IngestReplicatedAt.IsZero(), caller)
	assert.False(t, gf.IngestPreviousVersionExists, caller)
	assert.True(t, gf.IngestNeedsSave, caller)
	assert.Empty(t, gf.IngestErrorMessage, caller)
}
