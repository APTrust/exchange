package models_test

import (
	"github.com/APTrust/exchange/models"
	"testing"
)

// These tests cover the basic functions,
// but doesn't test synchronization.
// TODO: Test synchronization!

func TestNew(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	if syncMap == nil {
		t.Error("NewSynchronizedMap() returned nil")
	}
}

func TestHasKey(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	if syncMap.HasKey("does not exist") == true {
		t.Error("HasKey() should have returned false")
	}
	syncMap.Add("new key", "new value")
	if syncMap.HasKey("new key") == false {
		t.Error("HasKey() should have returned true")
	}
}

func TestAdd(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	syncMap.Add("new key", "new value")
	if syncMap.HasKey("new key") == false {
		t.Error("HasKey() should have returned true")
	}
}

func TestGet(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	if syncMap.Get("does not exist") != "" {
		t.Error("Get() should have returned empty string")
	}
	syncMap.Add("new key", "new value")
	if syncMap.Get("new key") != "new value" {
		t.Error("Get() returned '%s' instead of 'new value'", syncMap.Get("new key"))
	}
}

func TestDelete(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	syncMap.Add("new key", "new value")
	if syncMap.HasKey("new key") == false {
		t.Error("HasKey() should have returned true")
	}
	syncMap.Delete("new key")
	if syncMap.HasKey("new key") == true {
		t.Error("Delete() did not delete key 'new key'")
	}
}

func TestKeys(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	keys := syncMap.Keys()
	if len(keys) != 0 {
		t.Error("Keys() should have returned an empty list")
	}
	syncMap.Add("key 1", "value 1")
	syncMap.Add("key 2", "value 2")
	syncMap.Add("key 3", "value 3")
	keys = syncMap.Keys()
	if len(keys) != 3 {
		t.Error("Keys() should have returned 3 items")
	}
}

func TestValues(t *testing.T) {
	syncMap := models.NewSynchronizedMap()
	values := syncMap.Values()
	if len(values) != 0 {
		t.Error("Values() should have returned an empty list")
	}
	syncMap.Add("key 1", "value 1")
	syncMap.Add("key 2", "value 2")
	syncMap.Add("key 3", "value 3")
	values = syncMap.Values()
	if len(values) != 3 {
		t.Error("Values() should have returned 3 items")
	}
}
