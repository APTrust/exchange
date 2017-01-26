package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewStorageSummary(t *testing.T) {
	objIdentifier := "ncsu.edu/bag1"
	tarPath := "/tmp/ncsu.edu/bag1.tar"
	untarredPath := "/tmp/ncsu.edu/bag1"
	gf := testutil.MakeGenericFile(0, 0, objIdentifier)

	summary, err := models.NewStorageSummary(gf, tarPath, untarredPath)
	assert.Nil(t, err)
	assert.NotNil(t, summary)
	assert.NotNil(t, summary.StoreResult)
	assert.Equal(t, tarPath, summary.TarFilePath)
	assert.Equal(t, untarredPath, summary.UntarredPath)

	summary, err = models.NewStorageSummary(nil, tarPath, untarredPath)
	require.NotNil(t, err)
	assert.Equal(t, "Param gf cannot be nil", err.Error())

	summary, err = models.NewStorageSummary(gf, "", untarredPath)
	require.NotNil(t, err)
	assert.Equal(t, "Param tarPath cannot be empty", err.Error())

	// OK for untarredPath to be empty
	summary, err = models.NewStorageSummary(gf, tarPath, "")
	assert.Nil(t, err)
	assert.NotNil(t, summary)
}
