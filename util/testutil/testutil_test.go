package testutil_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
)

func TestLoadIntelObjFixture(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Error(err)
	}
	if obj.Identifier == "" {
		t.Errorf("Intellectual object identifier is missing")
	}
}
