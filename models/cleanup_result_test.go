package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
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
		return
	}
	err = json.Unmarshal(file, &result)
	if err != nil {
		t.Errorf("Error parson JSON from cleanup result test file '%s': %v", filepath, err)
		return
	}

	assert.True(t, result.Succeeded())
	for _, file := range result.Files {
		if file.DeleteAttempted() == false {
			assert.True(t, file.DeleteAttempted())
		}
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = "Spongebob"
	}

	assert.False(t, result.Succeeded())
	for _, file := range result.Files {
		assert.True(t, file.DeleteAttempted())
		// Set these for next test
		file.DeletedAt = time.Time{}
		file.ErrorMessage = ""
	}

	assert.False(t, result.Succeeded())
	for _, file := range result.Files {
		if file.DeleteAttempted() == true {
			// Delete not attempted, because DeletedAt == 0
			assert.False(t, file.DeleteAttempted())
		}
	}
}
