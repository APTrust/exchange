package storage_test

import (
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

}
