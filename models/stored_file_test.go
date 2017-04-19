package models_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestStoredFileToJson(t *testing.T) {
	f := testutil.MakeStoredFile()
	jsonString, err := f.ToJson()
	require.Nil(t, err)
	assert.NotEmpty(t, jsonString)
}

func TestStoredFileToStringArray(t *testing.T) {
	f := testutil.MakeStoredFile()
	s := f.ToStringArray()
	assert.Equal(t, 16, len(s))
	for i, str := range s {
		assert.NotEmpty(t, str, "String at %d should not be empty", i)
	}
}

func TestStoredFileToCSV(t *testing.T) {
	f := testutil.MakeStoredFile()
	csvString, err := f.ToCSV(',')
	require.Nil(t, err)
	assert.NotEmpty(t, csvString)
	assert.True(t, strings.Contains(csvString, ","))

	csvString, err = f.ToCSV('|')
	require.Nil(t, err)
	assert.NotEmpty(t, csvString)
	assert.True(t, strings.Contains(csvString, "|"))
}
