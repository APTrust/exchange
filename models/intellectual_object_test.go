package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
)

func assertValue(t *testing.T, data map[string]interface{}, key, expected string) {
	if data[key] != expected {
		t.Errorf("For key '%s', expected '%s' but found '%s'", key, expected, data[key])
	}
}

func TestSerializeForCreate(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filename)
	jsonBytes, err := obj.SerializeForCreate(-1)
	if err != nil {
		t.Error(err)
		return
	}

	// Translate the JSON back into a go map so we can test it.
	data := make([]map[string]interface{}, 1)
	err = json.Unmarshal(jsonBytes, &data)
	if err != nil {
		t.Error(err)
		return
	}

	// Intellectual object
	assertValue(t, data[0], "access", "institution")
	assertValue(t, data[0], "description", "A collection from Cincinnati")
	assertValue(t, data[0], "identifier", "uc.edu/cin.675812")
	assertValue(t, data[0], "institution_id", "uc.edu")

	// Intellectual object events
	objEvents := data[0]["premisEvents"].([]interface{})
	firstEvent := objEvents[0].(map[string]interface{})
	secondEvent := objEvents[1].(map[string]interface{})
	thirdEvent := objEvents[2].(map[string]interface{})
	assertValue(t, firstEvent, "type", "identifier_assignment")
	assertValue(t, firstEvent, "outcome", "Success")
	assertValue(t, secondEvent, "type", "ingest")
	assertValue(t, secondEvent, "outcome", "Success")
	assertValue(t, secondEvent, "outcome_detail", "2 files copied")
	assertValue(t, thirdEvent, "type", "access_assignment")
	assertValue(t, thirdEvent, "outcome", "Success")
	assertValue(t, thirdEvent, "outcome_detail", "institution")
	if len(objEvents) != 3 {
		t.Errorf("Expected 3 object events but found %d", len(objEvents))
	}

	// Generic files
	files := data[0]["generic_files"].([]interface{})
	file1 := files[0].(map[string]interface{})
	assertValue(t, file1, "created", "2014-04-25T18:05:51-05:00")
	assertValue(t, file1, "file_format", "text/plain")
	assertValue(t, file1, "identifier", "uc.edu/cin.675812/data/object.properties")
	assertValue(t, file1, "modified", "2014-04-25T18:05:51-05:00")
	assertValue(t, file1, "uri", "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/object.properties")
	if file1["size"] != float64(80) {
		t.Errorf("Expected file size 5105, got %f", file1["size"])
	}
	if len(files) != 2 {
		t.Errorf("Expected 2 generic files, found %d", len(files))
	}

	// Generic file checksums
	checksums := file1["checksum"].([]interface{})
	checksum2 := checksums[1].(map[string]interface{})
	assertValue(t, checksum2, "algorithm", "sha256")
	assertValue(t, checksum2, "datetime", "2014-08-12T20:51:20Z")
	assertValue(t, checksum2, "digest", "8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7")
	if len(checksums) != 2 {
		t.Errorf("Expected 2 checksums but found %d", len(checksums))
	}

	// Generic file events
	events := file1["premisEvents"].([]interface{})
	event1 := events[0].(map[string]interface{})
	event2 := events[1].(map[string]interface{})
	event3 := events[2].(map[string]interface{})
	event4 := events[3].(map[string]interface{})
	event5 := events[4].(map[string]interface{})

	assertValue(t, event1, "type", "fixity_check")
	assertValue(t, event1, "outcome_detail", "md5:8d7b0e3a24fc899b1d92a73537401805")

	assertValue(t, event2, "type", "ingest")
	assertValue(t, event2, "outcome_detail", "8d7b0e3a24fc899b1d92a73537401805")

	assertValue(t, event3, "type", "fixity_generation")
	assertValue(t, event3, "outcome_detail", "sha256:8373697fe955134036d758ee6bcf1077f74c20fe038dde3238f709ed96ae80f7")

	assertValue(t, event4, "type", "identifier_assignment")
	assertValue(t, event4, "outcome_detail", "uc.edu/cin.675812/data/object.properties")

	assertValue(t, event5, "type", "identifier_assignment")
	assertValue(t, event5, "outcome_detail", "https://s3.amazonaws.com/aptrust.test.preservation/3829076e-c322-4b53-4e80-0c1e93ea272e")

	if len(events) != 10 {
		t.Errorf("Expected 10 file events but found %d", len(events))
	}

}

func TestSerializeForCreateWithMaxFiles(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filename)
	jsonBytes, err := obj.SerializeForCreate(1)
	if err != nil {
		t.Error(err)
		return
	}

	// Translate the JSON back into a go map so we can test it.
	data := make([]map[string]interface{}, 1)
	err = json.Unmarshal(jsonBytes, &data)
	if err != nil {
		t.Error(err)
		return
	}

	// There should be only one generic file, since
	// we passed maxGenericFiles = 1
	files := data[0]["generic_files"].([]interface{})
	if len(files) != 1 {
		t.Error("JSON data from SerializeForCreate() should have had only one Generic File")
	}
}
