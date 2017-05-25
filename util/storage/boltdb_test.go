package storage_test

import (
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/storage"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestBoltDB(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "boltdb_test")
	require.Nil(t, err)
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	bolt, err := storage.NewBoltDB(tempFile.Name())
	require.Nil(t, err)
	defer bolt.Close()

	// Save and retrieve an object
	obj := testutil.MakeIntellectualObject(1, 1, 1, 10)
	err = bolt.Save("Test Object", obj)
	require.Nil(t, err)

	restoredObj, err := bolt.GetIntellectualObject("Test Object")
	require.Nil(t, err)
	require.NotNil(t, restoredObj)
	assert.Equal(t, obj.Identifier, restoredObj.Identifier)

	nilObj, err := bolt.GetIntellectualObject("Nil Object")
	require.Nil(t, err)
	require.Nil(t, nilObj)

	// Save and retrieve a generic file
	gf := testutil.MakeGenericFile(2, 2, "uc.edu/bag/data/file.json")

	err = bolt.Save(gf.Identifier, gf)
	require.Nil(t, err)

	restoredFile, err := bolt.GetGenericFile(gf.Identifier)
	require.Nil(t, err)
	require.NotNil(t, restoredFile)
	assert.Equal(t, gf.Identifier, restoredFile.Identifier)

	nilFile, err := bolt.GetGenericFile("Nil File")
	require.Nil(t, err)
	require.Nil(t, nilFile)

	// Get a list of keys
	keys := bolt.Keys()
	require.Equal(t, 2, len(keys))
	assert.True(t, util.StringListContains(keys, "Test Object"))
	assert.True(t, util.StringListContains(keys, gf.Identifier))
}
