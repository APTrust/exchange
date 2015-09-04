package results_test

import (
	"github.com/APTrust/exchange/results"
	"github.com/crowdmob/goamz/s3"
	"testing"
)

func TestNewS3FetchResultWithKey(t *testing.T) {
	key := s3.Key{
		Key: "yadda.yadda",
		Size: 54321,
		ETag: "81726354afdc",
	}
	s3result := results.NewS3FetchResultWithKey("bucket1", key)
	if s3result.BucketName != "bucket1" {
		t.Errorf("Expected bucket 'bucket1', got '%s'", s3result.BucketName)
	}
	if s3result.Key.Key != key.Key {
		t.Errorf("Expected key '%s', got '%s'", key.Key, s3result.Key.Key)
	}
	if s3result.Key.Size != key.Size {
		t.Errorf("Expected size %d, got %d", key.Size, s3result.Key.Size)
	}
	if s3result.Key.ETag != key.ETag {
		t.Errorf("Expected etag '%s', got '%s'", key.ETag, s3result.Key.ETag)
	}
	if s3result.Summary.Retry != true {
		t.Errorf("S3FetchResult.Summary was not properly initialized")
	}
}

func TestNewS3FetchResultWithName(t *testing.T) {
	s3result := results.NewS3FetchResultWithName("bucket1", "Key Wee")
	if s3result.BucketName != "bucket1" {
		t.Errorf("Expected bucket 'bucket1', got '%s'", s3result.BucketName)
	}
	if s3result.Key.Key != "Key Wee" {
		t.Errorf("Expected key 'Key Wee', got '%s'", s3result.Key.Key)
	}
	if s3result.Summary.Retry != true {
		t.Errorf("S3FetchResult.Summary was not properly initialized")
	}
}

func TestKeyIsComplete(t *testing.T) {
	s3result := results.NewS3FetchResultWithName("bucket1", "filename.txt")
	if s3result.KeyIsComplete() {
		t.Errorf("KeyIsComplete should have returned false")
	}
	s3result.Key.Size = 4800
	s3result.Key.ETag = "aec157cfbc1a34d52"
	if !s3result.KeyIsComplete() {
		t.Errorf("KeyIsComplete should have returned true")
	}
}
