package integration_test

import (
	//"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	//"github.com/APTrust/exchange/util"
	//"github.com/APTrust/exchange/util/storage"
	//"github.com/APTrust/exchange/util/testutil"
	//"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/require"
	//"path/filepath"
	"testing"
)

func TestIngestedItemsInPharos(t *testing.T) {
	//for _, bagName := range testutil.INTEGRATION_GOOD_BAGS {
	// Make sure Ingest WorkItem is present, and is
	// set to Stage = Cleanup, Status = Success,
	// Node = nil, Pid = 0. Also, get the object identifier
	// and run the object tests
	//}
}

func testObject(t *testing.T, objectIdentifier string) {
	// Make sure the object is present.
	// Make sure its attributes are correct
	// Make sure the object-level ingest events are present.
	// Make sure the right number of generic files were preserved.
	// Test all of the object's generic files.
}

func testFile(t *testing.T, gf *models.GenericFile) {
	// Test file properties.
	// Make sure all ingest events are present.
	// Make sure checksums are present.
	// Call testFileIsInStore for S3 and Glacier
}

func testFileIsInStorage(t *testing.T, fileUrl string) {
	// Make sure file is present
	// Make sure the size matches
	// Make sure all of the metadata attributes are set.
}
