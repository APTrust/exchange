package models_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChecksumMergeAttributes (t *testing.T) {
	cs1 := testutil.MakeChecksum()
	cs2 := testutil.MakeChecksum()

	err := cs1.MergeAttributes(cs2)
	require.Nil(t, err)
	assert.Equal(t, cs1.Id, cs2.Id)
	assert.Equal(t, cs1.CreatedAt, cs2.CreatedAt)
	assert.Equal(t, cs1.UpdatedAt, cs2.UpdatedAt)

	err = cs1.MergeAttributes(nil)
	assert.NotNil(t, err)
}
