package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// getPath returns the absolute path to a tar file in our unit test dir.
func getPath(file string) (string, error) {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	relDir := path.Join(dir, "..", "testdata", "unit_test_bags", file)
	return filepath.Abs(relDir)
}

func TestNewIngestManifest(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.NotNil(t, manifest.FetchResult)
	assert.NotNil(t, manifest.ValidateResult)
	assert.NotNil(t, manifest.StoreResult)
	assert.NotNil(t, manifest.RecordResult)
	assert.NotNil(t, manifest.CleanupResult)
	assert.NotNil(t, manifest.Object)
}

func TestIngestManifest_HasErrors(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasErrors())

	manifest.FetchResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.FetchResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.UntarResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.UntarResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.ValidateResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.ValidateResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.StoreResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.StoreResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.RecordResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.RecordResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.CleanupResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.CleanupResult.ClearErrors()
	assert.False(t, manifest.HasErrors())
}

func TestIngestManifest_HasFatalErrors(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasFatalErrors())

	manifest.FetchResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.FetchResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.UntarResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.UntarResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.ValidateResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.ValidateResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.StoreResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.StoreResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.RecordResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.RecordResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.CleanupResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.CleanupResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())
}

func TestIngestManifest_AllErrorsAsString(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasErrors())

	manifest.FetchResult.AddError("error 1")
	manifest.FetchResult.AddError("error 2")
	manifest.ValidateResult.AddError("error 3")
	manifest.StoreResult.AddError("error 4")
	manifest.RecordResult.AddError("error 5")
	manifest.CleanupResult.AddError("error 6")

	expected := "error 1\nerror 2\nerror 3\nerror 4\nerror 5\nerror 6\n"
	assert.Equal(t, expected, manifest.AllErrorsAsString())
}

func TestIngestManifest_BagIsOnDisk(t *testing.T) {
	goodPath, _ := getPath("example.edu.tagsample_good.tar")
	badPath, _ := getPath("i_do_not_exist.tar")
	manifest := models.NewIngestManifest()
	manifest.BagPath = ""
	assert.False(t, manifest.BagIsOnDisk())
	manifest.BagPath = goodPath
	assert.True(t, manifest.BagIsOnDisk())
	manifest.BagPath = badPath
	assert.False(t, manifest.BagIsOnDisk())
}

func TestIngestManifest_DBExists(t *testing.T) {
	goodPath, _ := getPath("example.edu.tagsample_good.tar")
	badPath, _ := getPath("i_do_not_exist.tar")
	manifest := models.NewIngestManifest()
	manifest.DBPath = ""
	assert.False(t, manifest.DBExists())
	manifest.DBPath = goodPath
	assert.True(t, manifest.DBExists())
	manifest.DBPath = badPath
	assert.False(t, manifest.DBExists())
}

func TestIngestManifest_SizeOfBagOnDisk(t *testing.T) {
	goodPath, _ := getPath("example.edu.tagsample_good.tar")
	badPath, _ := getPath("i_do_not_exist.tar")
	manifest := models.NewIngestManifest()
	manifest.BagPath = ""
	size, err := manifest.SizeOfBagOnDisk()
	assert.NotNil(t, err)
	assert.Equal(t, int64(-1), size)

	manifest.BagPath = badPath
	size, err = manifest.SizeOfBagOnDisk()
	assert.NotNil(t, err)
	assert.Equal(t, int64(-1), size)

	manifest.BagPath = goodPath
	size, err = manifest.SizeOfBagOnDisk()
	assert.Nil(t, err)
	assert.Equal(t, int64(40960), size)
}

func TestIngestManifest_ClearAllErrors(t *testing.T) {
	manifest := models.NewIngestManifest()
	manifest.FetchResult.AddError("1")
	manifest.UntarResult.AddError("2")
	manifest.ValidateResult.AddError("3")
	manifest.StoreResult.AddError("4")
	manifest.RecordResult.AddError("5")
	manifest.CleanupResult.AddError("6")
	assert.True(t, manifest.HasErrors())

	manifest.ClearAllErrors()
	assert.False(t, manifest.HasErrors())
}

func TestIngestManifest_BagHasBeenValidated(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.BagHasBeenValidated())
	manifest.ValidateResult.Attempted = true
	assert.False(t, manifest.BagHasBeenValidated())
	manifest.ValidateResult.FinishedAt = time.Now().UTC()
	manifest.ValidateResult.AddError("Not valid")
	assert.False(t, manifest.BagHasBeenValidated())
	manifest.ValidateResult.ClearErrors()
	assert.True(t, manifest.BagHasBeenValidated())
}

func TestIngestManifest_ObjectIdentifier(t *testing.T) {
	manifest := models.NewIngestManifest()
	manifest.S3Bucket = "aptrust.receiving.test.test.edu"
	manifest.S3Key = "test_bag.tar"
	objIdentifier, err := manifest.ObjectIdentifier()
	assert.Nil(t, err)
	assert.Equal(t, "test.edu/test_bag", objIdentifier)

	manifest.S3Bucket = "aptrust.receiving.virginia.edu"
	manifest.S3Key = "test_bag.b002.of014.tar"
	objIdentifier, err = manifest.ObjectIdentifier()
	assert.Nil(t, err)
	assert.Equal(t, "virginia.edu/test_bag", objIdentifier)

	manifest.S3Bucket = "xxx"
	manifest.S3Key = "test_bag.b002.of014.tar"
	objIdentifier, err = manifest.ObjectIdentifier()
	assert.NotNil(t, err)

	manifest.S3Bucket = "aptrust.receiving.virginia.edu"
	manifest.S3Key = ""
	objIdentifier, err = manifest.ObjectIdentifier()
	assert.NotNil(t, err)
}
