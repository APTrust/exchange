package workers_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Most worker tests are in the integration test suite
// under the /scripts directory.

func TestCloneWithoutSavedChildren(t *testing.T) {
	// Create GenericFile with 3 events and 3 checksums
	gf := testutil.MakeGenericFile(3, 3, "test.edu/file1.txt")
	require.Equal(t, 3, len(gf.PremisEvents))
	require.Equal(t, 3, len(gf.Checksums))

	// Id of zero means the item has not been saved in Pharos.
	for _, event := range gf.PremisEvents {
		event.Id = 0
	}
	for _, cs := range gf.Checksums {
		cs.Id = 0
	}

	// Should keep all unsaved events and checksums.
	firstClone := workers.CloneWithoutSavedChildren(gf)
	assert.Equal(t, 3, len(firstClone.PremisEvents))
	assert.Equal(t, 3, len(firstClone.Checksums))

	// Non-zero id means item has been saved.
	for _, event := range gf.PremisEvents {
		event.Id = 42
	}
	for _, cs := range gf.Checksums {
		cs.Id = 42
	}

	// Should keep all unsaved events and checksums.
	secondClone := workers.CloneWithoutSavedChildren(gf)
	assert.Equal(t, 0, len(secondClone.PremisEvents))
	assert.Equal(t, 0, len(secondClone.Checksums))

}
