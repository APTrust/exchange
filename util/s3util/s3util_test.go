package s3util_test

import (
	"github.com/APTrust/exchange/util/s3util"
	"github.com/crowdmob/goamz/s3"
	"testing"
)

func TestKeyIsComplete(t *testing.T) {
	key := s3.Key { Key: "filename.txt" }
	if s3util.KeyIsComplete(key) {
		t.Errorf("KeyIsComplete should have returned false")
	}
	key.Size = 4800
	key.ETag = "aec157cfbc1a34d52"
	if !s3util.KeyIsComplete(key) {
		t.Errorf("KeyIsComplete should have returned true")
	}
}
