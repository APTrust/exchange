package storage

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/boltdb/bolt"
	"io"
	"strings"
	"time"
)

const FILE_BUCKET = "files"
const OBJ_BUCKET = "objects"

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

// NewBoltDB opens a bolt database, creating the DB file if it doesn't
// already exist. The DB file is a key-value store that resides in a
// single file on disk.
func NewBoltDB(filePath string) (boltDB *BoltDB, err error) {
	db, err := bolt.Open(filePath, 0644, &bolt.Options{Timeout: 2 * time.Second})
	if err == nil {
		boltDB = &BoltDB{
			db:       db,
			filePath: filePath,
		}
		err = boltDB.initBuckets()
	}
	return boltDB, err
}

// Initialize a default bucket for the bolt DB. Since we're creating
// the DB for just one bag, and we know GenericFile identifiers within
// the bag will be unique, we can put everything in one bucket.
func (boltDB *BoltDB) initBuckets() error {
	err := boltDB.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(FILE_BUCKET))
		if err != nil {
			return fmt.Errorf("Error creating file bucket: %s", err)
		}
		_, err = tx.CreateBucketIfNotExists([]byte(OBJ_BUCKET))
		if err != nil {
			return fmt.Errorf("Error creating object bucket: %s", err)
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

// ObjectIdentifier returns the IntellectualObject.Identifier
// for the object stored in this DB file.
func (boltDB *BoltDB) ObjectIdentifier() string {
	key := make([]byte, 0)
	boltDB.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(OBJ_BUCKET))
		c := b.Cursor()
		key, _ = c.First()
		return nil
	})
	return string(key)
}

// Save saves a value to the bolt database.
func (boltDB *BoltDB) Save(key string, value interface{}) error {
	_, isIntelObj := value.(*models.IntellectualObject)
	bucketName := FILE_BUCKET
	if isIntelObj {
		bucketName = OBJ_BUCKET
	}
	var byteSlice []byte
	buf := bytes.NewBuffer(byteSlice)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(value)
	if err == nil {
		err = boltDB.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(bucketName))
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
		bucket := tx.Bucket([]byte(OBJ_BUCKET))
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
		bucket := tx.Bucket([]byte(FILE_BUCKET))
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
// file bucket.
func (boltDB *BoltDB) ForEach(fn func(k, v []byte) error) error {
	var err error
	return boltDB.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(FILE_BUCKET))
		err = bucket.ForEach(fn)
		if err != nil {
			return err
		}
		return nil
	})
}

// FileIdentifiers returns a list of all keys in the database.
func (boltDB *BoltDB) FileIdentifiers() []string {
	keys := make([]string, 0)
	boltDB.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_BUCKET))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})
	return keys
}

// FileCount returns the number of GenericFiles stored in the database.
func (boltDB *BoltDB) FileCount() int {
	count := 0
	boltDB.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_BUCKET))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count += 1
		}
		return nil
	})
	return count
}

// FileIdentifierBatch returns a list of GenericFile
// identifiers from offset (zero-based) up to limit,
// or end of list.
func (boltDB *BoltDB) FileIdentifierBatch(offset, limit int) []string {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = 0
	}
	index := 0
	end := offset + limit
	keys := make([]string, 0)
	boltDB.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_BUCKET))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if index >= offset && index < end {
				keys = append(keys, string(k))
			}
			index++
		}
		return nil
	})
	return keys
}

// DumpJson writes all the records from the db into a single
// JSON string. The output is the JSON representation of an
// IntellectualObject with all of its GenericFiles (and Checksums
// and PremisEvents, if there are any).
func (boltDB *BoltDB) DumpJson(writer io.Writer) error {
	objIdentifier := boltDB.ObjectIdentifier()
	obj, err := boltDB.GetIntellectualObject(objIdentifier)
	if err != nil {
		return fmt.Errorf("Can't get object from db: %v", err)
	}
	objBytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Errorf("Can't convert object to JSON: %v", err)
	}
	objJson := strings.TrimSpace(string(objBytes))

	// Catch case of null object. This happens if the bag was not
	// parsable.
	if objJson == "null" {
		objJson = `{ "identifier": "The bag could not be parsed"  `
	}

	// Normally, we'd just add the generic files to the object
	// and serialize the whole thing, but when we have 200k files,
	// that causes an out-of-memory exception. So this hack...
	// Cut off the closing curly bracket, dump in the GenericFiles
	// one by one, and then re-add the curly bracket.
	objJson = objJson[:len(objJson)-2] + ",\n"
	objJson += `  "generic_files": [`
	_, err = writer.Write([]byte(objJson))
	if err != nil {
		return fmt.Errorf("Error writing output: %v", err)
	}

	// Write out the GenericFiles one by one, without reading them
	// all into memory.
	count := 0
	err = boltDB.ForEach(func(k, v []byte) error {
		if string(k) != objIdentifier {
			gf := &models.GenericFile{}
			buf := bytes.NewBuffer(v)
			decoder := gob.NewDecoder(buf)
			err = decoder.Decode(gf)
			if err != nil {
				return fmt.Errorf("Error reading GenericFile from DB: %v", err)
			}
			gfBytes, err := json.MarshalIndent(gf, "    ", "  ")
			if err != nil {
				return fmt.Errorf("Can't convert generic file to JSON: %v", err)
			}
			if count > 0 {
				writer.Write([]byte(",\n    "))
			}
			writer.Write(gfBytes)
			count++
		}
		return nil
	})

	// Close up the JSON
	writer.Write([]byte("\n  ]\n}\n"))

	return err
}
