package results_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/results"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/crowdmob/goamz/s3"
	"path/filepath"
	"strings"
	"testing"
	"time"
)


// Returns a basic IngestResult that can be altered for
// specific tests.
func baseResult() (result *results.IngestResult) {
	result = &results.IngestResult{}
	result.S3File = &models.S3File{}
	result.S3File.BucketName = "aptrust.receiving.unc.edu"
	result.S3File.Key = s3.Key{}
	result.S3File.Key.Key = "sample.tar"
	result.S3File.Key.ETag = "\"0123456789ABCDEF\""
	result.S3File.Key.LastModified = "2014-05-28T16:22:24.016Z"
	result.Summary = results.NewSummary()
	return result
}

// Returns a result with Stage set to stage. If successful is false,
// the result will include an error message.
func getResult(stage constants.StageType, successful, retry bool) (result *results.IngestResult) {
	result = baseResult()
	if successful == false {
		result.Summary.AddError("Sample error message. Sumpin went rawng!")
	}
	result.Stage = stage
	result.Summary.Retry = retry
	return result
}

func loadIngestResultFixture(t *testing.T) *results.IngestResult {
	relativePath := filepath.Join("testdata", "ingest_result.json")
	absPath, err := fileutil.RelativeToAbsPath(relativePath)
	if err != nil {
		t.Error(err)
		return nil
	}
	ingestResult := &results.IngestResult{}
	err = fileutil.JsonFileToObject(absPath, ingestResult)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", absPath, err)
		return nil
	}
	return ingestResult
}

// Make sure Ingestresult.IngestStatus() returns the correct
// Ingestresult data.
func TestIngestStatus(t *testing.T) {
	passedFetch := getResult("Fetch", true, true)
	assertCorrectSummary(t, passedFetch, constants.StatusPending)
	startedFetch := getResult("Fetch", false, true)
	assertCorrectSummary(t, startedFetch, constants.StatusStarted)
	failedFetch := getResult("Fetch", false, false)
	assertCorrectSummary(t, failedFetch, constants.StatusFailed)

	passedUnpack := getResult("Unpack", true, true)
	assertCorrectSummary(t, passedUnpack, constants.StatusPending)
	startedUnpack := getResult("Unpack", false, true)
	assertCorrectSummary(t, startedUnpack, constants.StatusStarted)
	failedUnpack := getResult("Unpack", false, false)
	assertCorrectSummary(t, failedUnpack, constants.StatusFailed)

	passedStore := getResult("Store", true, true)
	assertCorrectSummary(t, passedStore, constants.StatusPending)
	startedStore := getResult("Store", false, true)
	assertCorrectSummary(t, startedStore, constants.StatusStarted)
	failedStore := getResult("Store", false, false)
	assertCorrectSummary(t, failedStore, constants.StatusFailed)

	passedRecord := getResult("Record", true, true)
	assertCorrectSummary(t, passedRecord, constants.StatusSuccess)
	startedRecord := getResult("Record", false, true)
	assertCorrectSummary(t, startedRecord, constants.StatusStarted)
	failedRecord := getResult("Record", false, false)
	assertCorrectSummary(t, failedRecord, constants.StatusFailed)
}

func assertCorrectSummary(t *testing.T, result *results.IngestResult, expectedStatus constants.StatusType) {
	status := result.IngestStatus()
	expectedBagDate := "2014-05-28 16:22:24.016 +0000 UTC"
	if status.Date.IsZero() {
		t.Error("Ingestresult.Date was not set")
	}
	if status.Action != "Ingest" {
		t.Error("Ingestresult.Type is incorrect. Should be Ingest.")
	}
	if status.Name != result.S3File.Key.Key {
		t.Errorf("Ingestresult.Name: Expected %s, got %s",
			result.S3File.Key.Key,
			status.Name)
	}
	if status.Bucket != result.S3File.BucketName {
		t.Errorf("Ingestresult.Bucket: Expected %s, got %s",
			result.S3File.BucketName,
			status.Bucket)
	}
	if status.BagDate.String() != expectedBagDate {
		t.Errorf("Ingestresult.BagDate: Expected %s, got %s",
			expectedBagDate,
			status.BagDate)
	}
	if status.ETag != strings.Replace(result.S3File.Key.ETag, "\"", "", 2) {
		t.Errorf("Ingestresult.ETag: Expected %s, got %s",
			result.S3File.Key.ETag,
			status.ETag)
	}
	if status.Stage != result.Stage {
		t.Errorf("Ingestresult.Stage: Expected %s, got %s",
			result.Stage,
			status.Stage)
	}
	if status.Institution != util.OwnerOf(result.S3File.BucketName) {
		t.Errorf("Ingestresult.Institution: Expected %s, got %s",
			util.OwnerOf(result.S3File.BucketName),
			status.Institution)
	}
	if !result.Summary.HasErrors() && status.Note != "No problems" {
		t.Errorf("Ingestresult.Note should be '%s', but it's '%s'.",
			"No problems", status.Note)
	}
	if result.Summary.HasErrors() && status.Note == "" {
		t.Error("Ingestresult.Note should have a value, but it's empty.")
	}
	if result.Summary.HasErrors() && status.Note != result.Summary.FirstError() {
		t.Errorf("Ingestresult.Note: Expected %s, got %s",
			result.Summary.FirstError(),
			status.Note)
	}
	if status.Status != expectedStatus {
		t.Errorf("Ingestresult.Status: Expected %s, got %s",
			expectedStatus,
			status.Status)
		t.Errorf("This failure may be due to a temporary demo setting that considers Validation the final step.")
	}
}


func TestIntellectualObject(t *testing.T) {
	result := loadIngestResultFixture(t)
	if result == nil {
		return
	}
	obj, err := result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}
	if obj.Title != "Title of an Intellectual Object" {
		t.Errorf("IntellectualObject.Title is '%s', expected '%s'.",
			obj.Title,
			"Title of an Intellectual Object")
	}
	if obj.Description != "Description of intellectual object." {
		t.Errorf("IntellectualObject.Description is '%s', expected '%s'.",
			obj.Description,
			"Description of intellectual object.")
	}
	if obj.Identifier != "ncsu.edu/ncsu.1840.16-2928" {
		t.Errorf("IntellectualObject.Identifier is '%s', expected '%s'.",
			obj.Identifier,
			"ncsu.edu.ncsu.1840.16-2928")
	}
	if len(obj.AltIdentifier) != 1 {
		t.Errorf("IntellectualObject is missing AltIdentifier")
	}
	if len(obj.AltIdentifier) > 0 && obj.AltIdentifier[0] != "ncsu-internal-id-0001" {
		t.Errorf("IntellectualObject.AltIdentifier is %s, expected %s",
			obj.AltIdentifier[0], "ncsu-internal-id-0001")
	}
	if obj.Access != "consortia" {
		t.Errorf("IntellectualObject.Access is '%s', expected '%s'.",
			obj.Access,
			"consortia")
	}

	// Special test for Identifier
	result.S3File.Key.Key = "ncsu.1840.16-2928-blah.b12.of79.tar"
	obj, err = result.IntellectualObject()
	if err != nil {
		t.Errorf("Error creating intellectual object from result: %v", err)
	}
	if obj.Identifier != "ncsu.edu/ncsu.1840.16-2928-blah" {
		t.Errorf("IntellectualObject.Identifier is '%s', expected '%s'.",
			obj.Identifier,
			"ncsu.edu.ncsu.1840.16-2928-blah")
	}
}

func TestGenericFiles(t *testing.T) {
	result := loadIngestResultFixture(t)
	if result == nil {
		return
	}
	emptyTime := time.Time{}
	genericFiles, err := result.GenericFiles()
	if err != nil {
		t.Errorf("Error creating generic files from result: %v", err)
	}
	for _, gf := range genericFiles {
		if gf.URI == "" {
			t.Error("GenericFile.URI should not be nil")
		}
		if gf.Size <= 0 {
			t.Error("GenericFile.Size should be greater than zero")
		}
		if gf.Created == emptyTime {
			t.Error("GenericFile.Created should not be nil")
		}
		if gf.Modified == emptyTime {
			t.Error("GenericFile.Modified should not be nil")
		}
		if gf.Id != "" {
			t.Errorf("GenericFile.Id should be empty, but it's '%s'", gf.Id)
		}
		if strings.Index(gf.Identifier, "/") < 0 {
			t.Errorf("GenericFile.Identifier should contain slashes")
		}
		if strings.Index(gf.Identifier, "ncsu.edu") < 0 {
			t.Errorf("GenericFile.Identifier should contain the owner's domain name")
		}
		if strings.Index(gf.Identifier, strings.Replace(gf.Identifier, "/", "", -1)) > -1 {
			t.Errorf("GenericFile.Identifier should contain the file name")
		}
		for _, cs := range gf.ChecksumAttributes {
			if cs.Algorithm != "md5" && cs.Algorithm != "sha256" {
				t.Error("ChecksumAttribute.Algorithm should be either 'md5' or 'sha256'")
			}
			if cs.DateTime == emptyTime {
				t.Error("ChecksumAttribute.DateTime should not be nil")
			}
			if len(cs.Digest) == 0 {
				t.Error("ChecksumAttribute.Digest is empty")
			}

		}
	}

	// Look more closely at one GenericFile
	// A normal generic file URI would end with a UUID, but this
	// is an actual file in a fixture bucket used for testing.
	gf1 := genericFiles[0]
	if gf1.URI != "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml" {
		t.Errorf("GenericFile.URI is '%s', expected '%s'",
			gf1.URI,
			"https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml")
	}
	if gf1.Size != 5105 {
		t.Errorf("GenericFile.Size is %d, expected %d", gf1.Size, 5105)
	}
	// We can't get created time, so we're using modifed timstamp
	// for both created and modified
	modified, _ := time.Parse("2006-01-02T15:04:05Z", "2014-04-25T18:05:51Z")
	if gf1.Created != modified {
		t.Errorf("GenericFile.Created is %s, expected %d",
			gf1.Created,
			"0001-01-01T00:00:00Z")
	}
	if gf1.Modified != modified {
		t.Errorf("GenericFile.Modified is %s, expected %s",
			gf1.Modified,
			"2014-04-25T18:05:51Z")
	}

	// Test the checksums
	if gf1.ChecksumAttributes[0].Algorithm != "md5" {
		t.Errorf("ChecksumAttribute.Algorithm should be either 'md5'")
	}
	if gf1.ChecksumAttributes[0].Digest != "84586caa94ff719e93b802720501fcc7" {
		t.Errorf("ChecksumAttribute.Digest is %s, expected %s",
			gf1.ChecksumAttributes[0].Digest,
			"84586caa94ff719e93b802720501fcc7")
	}
	// MD5 checksum date is the modified date, since S3 calculates it
	// when the tar file is uploaded to the receiving bucket
	if gf1.ChecksumAttributes[0].DateTime != modified {
		t.Errorf("ChecksumAttributes.Date is %s, expected %s",
			gf1.ChecksumAttributes[0].DateTime,
			"2014-04-25T19:01:20.000Z")
	}

	if gf1.ChecksumAttributes[1].Algorithm != "sha256" {
		t.Errorf("ChecksumAttribute.Algorithm should be either 'md5'")
	}
	if gf1.ChecksumAttributes[1].Digest != "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784" {
		t.Errorf("ChecksumAttribute.Digest is %s, expected %s",
			gf1.ChecksumAttributes[1].Digest,
			"84586caa94ff719e93b802720501fcc7")
	}
	shaTime, _ := time.Parse("2006-01-02T15:04:05Z", "2014-06-09T14:12:45.574358959Z")
	if gf1.ChecksumAttributes[1].DateTime != shaTime {
		t.Errorf("ChecksumAttributes.Date is %s, expected %s",
			gf1.ChecksumAttributes[1].DateTime,
			"2014-06-09T14:12:45.574358959Z")
	}
}

func TestProcessResultPremisEvents(t *testing.T) {
	result := loadIngestResultFixture(t)
	if result == nil {
		return
	}
	emptyTime := time.Time{}
	genericFiles, err := result.GenericFiles()
	if err != nil {
		t.Errorf("Error creating generic files from result: %v", err)
	}
	for i, file := range genericFiles {
		if file.Events[0].EventType != "fixity_check" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[0].EventType,
				"fixity_check")
		}
		if file.Events[0].DateTime == emptyTime {
			t.Errorf("Event.DateTime is missing")
		}
		if file.Events[0].Identifier == "" {
			t.Errorf("Fixity check event identifier is missing")
		}

		if file.Events[1].EventType != "ingest" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[1].EventType,
				"ingest")
		}
		if file.Events[1].DateTime != result.TarResult.LocalFiles[i].StoredAt {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[1].DateTime,
				result.TarResult.LocalFiles[i].StoredAt)
		}
		if file.Events[1].OutcomeDetail != result.TarResult.LocalFiles[i].StorageMd5 {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[1].OutcomeDetail,
				result.TarResult.LocalFiles[i].StorageMd5)
		}
		if file.Events[1].Identifier == "" {
			t.Errorf("Ingest event identifier is missing")
		}

		if file.Events[2].EventType != "fixity_generation" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[2].EventType,
				"fixity_generation")
		}
		if file.Events[2].DateTime != result.TarResult.LocalFiles[i].Sha256Generated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[2].DateTime,
				result.TarResult.LocalFiles[i].Sha256Generated)
		}
		expected256 := fmt.Sprintf("sha256:%s", result.TarResult.LocalFiles[i].Sha256)
		if file.Events[2].OutcomeDetail != expected256 {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[2].OutcomeDetail,
				expected256)
		}
		if file.Events[2].Identifier == "" {
			t.Errorf("Fixity generation event id is missing")
		}

		if file.Events[3].EventType != "identifier_assignment" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[3].EventType,
				"identifier_assignment")
		}
		if file.Events[3].DateTime != result.TarResult.LocalFiles[i].UuidGenerated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[3].DateTime,
				result.TarResult.LocalFiles[i].UuidGenerated)
		}
		if file.Events[3].OutcomeDetail != result.TarResult.LocalFiles[i].Identifier {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[3].OutcomeDetail,
				result.TarResult.LocalFiles[i].Identifier)
		}
		if file.Events[3].Identifier == "" {
			t.Errorf("Identifier assignement event id is missing")
		}

		if file.Events[4].EventType != "identifier_assignment" {
			t.Errorf("EventType is '%s', expected '%s'",
				file.Events[4].EventType,
				"identifier_assignment")
		}
		if file.Events[4].DateTime != result.TarResult.LocalFiles[i].UuidGenerated {
			t.Errorf("DateTime is %v, expected %v",
				file.Events[4].DateTime,
				result.TarResult.LocalFiles[i].UuidGenerated)
		}
		if file.Events[4].OutcomeDetail != result.TarResult.LocalFiles[i].StorageURL {
			t.Errorf("OutcomeDetail is '%s', expected '%s'",
				file.Events[4].OutcomeDetail,
				result.TarResult.LocalFiles[i].StorageURL)
		}
		if file.Events[4].Identifier == "" {
			t.Errorf("Identifier assignement event id is missing")
		}
	}
}
