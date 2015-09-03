package testutil

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"encoding/json"
)


// Our test fixture describes a bag that includes the following file paths
var ExpectedPaths [4]string = [4]string{
	"data/metadata.xml",
	"data/object.properties",
	"data/ORIGINAL/1",
	"data/ORIGINAL/1-metadata.xml",
}

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
