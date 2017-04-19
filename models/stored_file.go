package models

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strconv"
	"time"
)

// StoredFile represents a file stored in a long-term storage
// bucket on S3 or Glacier. This object is used primarily for
// audit purposes, when we occasionally scan through our S3
// and Glacier buckets to get a list of what is actually stored
// there.
type StoredFile struct {
	// Id is a unique identifier for this StoredFile,
	// if it happens to be stored in a SQL database.
	// This can be zero for items not stored in a DB.
	Id int64 `json:"id"`
	// Key is the s3/glacier name of the file. A file
	// may be stored under more than one UUID if multiple
	// versions of it exist. Typically, we should retain
	// only the most recent version.
	Key string `json:"key"`
	// Bucket is the name of the bucket where the item is stored.
	Bucket string `json:"bucket"`
	// Size is the size, in bytes, of the object in
	// long-term storage. This should match the size
	// of the file in the GenericFiles table in Pharos.
	Size int64 `json:"size"`
	// ContentType is the object's mime type. E.g. image/jpeg.
	ContentType string `json:"content_type"`
	// Institution is the domain name of the institution
	// that owns the file. E.g. virginia.edu
	Institution string `json:"institution"`
	// BagName is the name of the bag this file belongs to.
	// It's the same as the intellectual object identifier.
	BagName string `json:"bag_name"`
	// PathInBag is the file's path in the original bag.
	// For examople, data/subdir/image.jpeg. Combine this
	// with BagName to get the GenericDile.Identifier.
	PathInBag string `json:"path_in_bag"`
	// Md5 is the md5 sum we calculated for this file at ingest.
	Md5 string `json:"md5"`
	// Sha256 is the sha256 checksum we calculated for this
	// file on ingest.
	Sha256 string `json:"sha256"`
	// ETag is Amazon's etag for this item. For multipart
	// uploads, the etag will contain a dash.
	ETag string `json:"etag"`
	// LastModified is when this file was last modified in
	// the long-term storage bucket.
	LastModified time.Time `json:"last_modified"`
	// LastSeenAt is when our system last saw this item in
	// the long-term storage bucket.
	LastSeenAt time.Time `json:"last_seen_at"`
	// CreatedAt is when this record was created.
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is when this record was updated.
	UpdatedAt time.Time `json:"updated_at"`
	// DeletedAt is when this file was deleted from long-term
	// storage. This will almost always be an empty timestamp.
	DeletedAt time.Time `json:"deleted_at,omitempty"`
}

// ToJson converts this object to JSON.
func (f *StoredFile) ToJson() (string, error) {
	jsonString, err := json.Marshal(f)
	return string(jsonString), err
}

// ToCSV converts this object to a CSV record.
// When listing thousands of files, we dump records
// to a CSV file that we can import later to a SQL DB.
// Param delimiter is the field delimiter (comma, tab, pipe, etc).
func (f *StoredFile) ToCSV(delimiter rune) (string, error) {
	buf := make([]byte, 0)
	buffer := bytes.NewBuffer(buf)
	writer := csv.NewWriter(buffer)
	writer.Comma = delimiter
	writer.Write(f.ToStringArray())
	writer.Flush()
	return buffer.String(), writer.Error()
}

// ToStringArray converts this record to a string array,
// usually so it can be serialized to CSV format.
func (f *StoredFile) ToStringArray() []string {
	s := make([]string, 16)
	s[0] = strconv.FormatInt(f.Id, 10)
	s[1] = f.Key
	s[2] = f.Bucket
	s[3] = strconv.FormatInt(f.Size, 10)
	s[4] = f.ContentType
	s[5] = f.Institution
	s[6] = f.BagName
	s[7] = f.PathInBag
	s[8] = f.Md5
	s[9] = f.Sha256
	s[10] = f.ETag
	s[11] = f.LastModified.Format(time.RFC3339)
	s[12] = f.LastSeenAt.Format(time.RFC3339)
	s[13] = f.CreatedAt.Format(time.RFC3339)
	s[14] = f.UpdatedAt.Format(time.RFC3339)
	s[15] = f.DeletedAt.Format(time.RFC3339)
	return s
}
