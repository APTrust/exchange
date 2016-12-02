package models

import (
	"sync"
)

// SynchronizedMap is a map structure that can be shared
// across go routines and threads. Both keys and values
// are strings.
type SynchronizedMap struct {
	data  map[string]string
	mutex *sync.RWMutex
}

// NewSynchronizedMap creates a new empty SynchronizedMap
func NewSynchronizedMap() *SynchronizedMap {
	return &SynchronizedMap{
		data:  make(map[string]string),
		mutex: &sync.RWMutex{},
	}
}

// HasKey returns true if the key exists in the map.
func (syncMap *SynchronizedMap) HasKey(key string) bool {
	syncMap.mutex.RLock()
	_, hasKey := syncMap.data[key]
	syncMap.mutex.RUnlock()
	return hasKey
}

// Add adds a key/value pair to the map.
func (syncMap *SynchronizedMap) Add(key, value string) {
	syncMap.mutex.Lock()
	syncMap.data[key] = value
	syncMap.mutex.Unlock()
}

// Get returns the value of key from the map.
func (syncMap *SynchronizedMap) Get(key string) string {
	syncMap.mutex.RLock()
	value, _ := syncMap.data[key]
	syncMap.mutex.RUnlock()
	return value
}

// Delete deletes the specified key from the map.
func (syncMap *SynchronizedMap) Delete(key string) {
	syncMap.mutex.Lock()
	delete(syncMap.data, key)
	syncMap.mutex.Unlock()
}

// Keys returns a slice of all keys in the map.
func (syncMap *SynchronizedMap) Keys() []string {
	keys := make([]string, len(syncMap.data))
	syncMap.mutex.RLock()
	i := 0
	for key := range syncMap.data {
		keys[i] = key
		i += 1
	}
	syncMap.mutex.RUnlock()
	return keys
}

// Values returns a slice of all values in the map.
func (syncMap *SynchronizedMap) Values() []string {
	vals := make([]string, len(syncMap.data))
	syncMap.mutex.RLock()
	i := 0
	for _, val := range syncMap.data {
		vals[i] = val
		i += 1
	}
	syncMap.mutex.RUnlock()
	return vals
}
