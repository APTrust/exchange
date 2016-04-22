package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
	"time"
)

// Bloomsday
var TEST_TIMESTAMP time.Time = time.Date(2016, 6, 16, 10, 24, 16, 0, time.UTC)

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

func TestSerializeObjectForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	data, err := intelObj.SerializeForPharos()
	if err != nil {
		t.Errorf("Error serializing for Pharos: %v", err)
		return
	}
	hash := make(map[string]interface{})
	err = json.Unmarshal(data, &hash)
	if err != nil {
		t.Errorf("Error unmarshalling data: %v", err)
	}

	assertValue(t, "TestSerializeObjectForPharos", hash, "identifier", "uc.edu/cin.675812")
	assertValue(t, "TestSerializeObjectForPharos", hash, "bag_name", "cin.675812")
	assertValue(t, "TestSerializeObjectForPharos", hash, "institution", "uc.edu")
	assertValue(t, "TestSerializeObjectForPharos", hash, "title", "Notes from the Oesper Collections")
	assertValue(t, "TestSerializeObjectForPharos", hash, "description", "A collection from Cincinnati")
	assertValue(t, "TestSerializeObjectForPharos", hash, "alt_identifier", "Photo Collection")
	assertValue(t, "TestSerializeObjectForPharos", hash, "access", "institution")
}
