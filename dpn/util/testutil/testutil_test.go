package testutil_test

import (
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMakeDPNStoredFile(t *testing.T) {
	f := testutil.MakeDPNStoredFile()
	require.NotNil(t, f)
	assert.NotEmpty(t, f.Id)
	assert.NotEmpty(t, f.Key)
	assert.NotEmpty(t, f.Bucket)
	assert.NotEmpty(t, f.Size)
	assert.NotEmpty(t, f.ContentType)
	assert.NotEmpty(t, f.Member)
	assert.NotEmpty(t, f.FromNode)
	assert.NotEmpty(t, f.TransferId)
	assert.NotEmpty(t, f.LocalId)
	assert.NotEmpty(t, f.Version)
	assert.NotEmpty(t, f.ETag)
	assert.NotEmpty(t, f.LastModified)
	assert.NotEmpty(t, f.LastSeenAt)
	assert.NotEmpty(t, f.CreatedAt)
	assert.NotEmpty(t, f.UpdatedAt)
	assert.NotEmpty(t, f.DeletedAt)
}
