package testutil

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"encoding/json"
)

// Loads an IntellectualObject fixture (a JSON file) from
// the testdata directory for testing.
func LoadIntelObjFixture(filename string) (*models.IntellectualObject, error) {
	data, err := fileutil.LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	intelObj := &models.IntellectualObject{}
	err = json.Unmarshal(data, intelObj)
	if err != nil {
		return nil, err
	}
	return intelObj, nil
}
