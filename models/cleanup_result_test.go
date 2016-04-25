package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/models"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func TestDeleteAttemptedAndSucceeded(t *testing.T) {
	// TODO: Need a more reliable way to get path to test data file
	filepath := filepath.Join("..", "testdata", "cleanup_result.json")
	var result models.CleanupResult
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("Error loading cleanup result test file '%s': %v", filepath, err)
	}

	if result.Succeeded() == false {
		t.Error("result.Succeeded() should have returned true")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = "Spongebob"
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			t.Error("file.DeleteAttempted() should have returned true")
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = ""
	}

	if result.Succeeded() == true {
		t.Error("result.Succeeded() should have returned false")
	}
	for _, file := range result.Files {
		if file.DeleteAttempted() == true {
			t.Error("file.DeleteAttempted() should have returned false")
		}
	}
}
