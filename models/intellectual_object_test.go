package models_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
)

func assertValue(t *testing.T, data map[string]interface{}, key, expected string) {
	if data[key] != expected {
		t.Errorf("For key '%s', expected '%s' but found '%s'", key, expected, data[key])
	}
}

func TestTotalFileSize(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}
