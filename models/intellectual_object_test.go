package models_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
)

// Assert that a value in a map is what we expect.
// Convert int and int64 to float64, because that's what JSON uses
func assertValue(t *testing.T, testName string, data map[string]interface{}, key string, expected interface{}) {
	if data[key] != expected {
		t.Errorf("[%s] For key '%s', expected '%s' but found '%s'", testName, key, expected, data[key])
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

func TestSerializeForFluctus(t *testing.T) {

}
