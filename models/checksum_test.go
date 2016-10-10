package models_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
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

func TestChecksumSerializeForPharos (t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	checksum := intelObj.GenericFiles[0].Checksums[0]
	jsonData, err := checksum.SerializeForPharos()
	expected := `{"checksum":{"generic_file_id":0,"algorithm":"md5","datetime":"2014-04-25T18:05:51Z","digest":"8d7b0e3a24fc899b1d92a73537401805"}}`
	assert.Equal(t, expected, string(jsonData))
}
