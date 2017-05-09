package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/boltdb/bolt"
)

const DEFAULT_BUCKET = "default"

// BoltDB represents a bolt database, which is a single-file key-value
// store. Our validator uses this to track information about the files
// inside a bag that we're validating. At a minimum, the validator
// typically needs to track these pieces of information for each file:
// the absolute path, the manifests' md5 digest, the manifest's sha256
// digest, the validator's calculated md5 digest, and the validator's
// calculated sha256 digest. That can be a few hundred bytes of data
// per file. APTrust ingest services will track more than that: about
// 8-9 kilobytes of data per file. Multiply that by 100k or even
// 1 million files in a bag, and that's too much to keep in memory.
type BoltDB struct {
	db       *bolt.DB
	filePath string
}

// NewBoltDB creates a new bolt database, which is a key-value store
// that resides in a single file on disk.
func NewBoltDB(filePath string) (boltDB *BoltDB, err error) {
	db, err := bolt.Open(filePath, 0644, nil)
	if err == nil {
		boltDB = &BoltDB{
			db:       db,
			filePath: filePath,
		}
		err = boltDB.initDefaultBucket()
	}
	return boltDB, err
}

// Initialize a default bucket for the bolt DB. Since we're creating
// the DB for just one bag, and we know GenericFile identifiers within
// the bag will be unique, we can put everything in one bucket.
func (boltDB *BoltDB) initDefaultBucket() error {
	err := boltDB.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(DEFAULT_BUCKET))
		if err != nil {
			return fmt.Errorf("Error creating bucket: %s", err)
		}
		return nil
	})
	return err
}

// FilePath returns the path to the bolt DB file.
func (boltDB *BoltDB) FilePath() string {
	return boltDB.filePath
}

// Close closes the bolt database.
func (boltDB *BoltDB) Close() {
	boltDB.db.Close()
}

// Save saves a value to the bolt database.
func (boltDB *BoltDB) Save(key string, value interface{}) error {
	var byteSlice []byte
	buf := bytes.NewBuffer(byteSlice)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(value)
	if err == nil {
		err = boltDB.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(DEFAULT_BUCKET))
			err := bucket.Put([]byte(key), buf.Bytes())
			return err
		})
	}
	return err
}

// GetIntellectualObject returns the IntellectualObject that matches
// the specified key. This object will NOT include GenericFiles.
// There may be tens of thousands of those, so you have to fetch
// them individually. Param key is the IntellectualObject.Identifier.
// If key is not found, this returns nil and no error.
func (boltDB *BoltDB) GetIntellectualObject(key string) (*models.IntellectualObject, error) {
	var err error
	obj := &models.IntellectualObject{}
	err = boltDB.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DEFAULT_BUCKET))
		value := bucket.Get([]byte(key))
		if len(value) > 0 {
			buf := bytes.NewBuffer(value)
			decoder := gob.NewDecoder(buf)
			err = decoder.Decode(obj)
		} else {
			obj = nil
		}
		return err
	})
	return obj, err
}

// GetGenericFile returns the GenericFile with the specified identifier.
// The GenericFile will include checksums and events, if they are available.
// Param key is the GenericFile.Identifier. If key is not found this returns
// nil and no error.
func (boltDB *BoltDB) GetGenericFile(key string) (*models.GenericFile, error) {
	var err error
	gf := &models.GenericFile{}
	err = boltDB.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DEFAULT_BUCKET))
		value := bucket.Get([]byte(key))
		if len(value) > 0 {
			buf := bytes.NewBuffer(value)
			decoder := gob.NewDecoder(buf)
			err = decoder.Decode(gf)
		} else {
			gf = nil
		}
		return err
	})
	return gf, err
}

// ForEach calls the specified function for each key in the database's
// default bucket.
func (boltDB *BoltDB) ForEach(fn func(k, v []byte) error) error {
	var err error
	return boltDB.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DEFAULT_BUCKET))
		err = bucket.ForEach(fn)
		if err != nil {
			return err
		}
		return nil
	})
}
