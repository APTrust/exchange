package models_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/testdata"
	"testing"
)

func getLocalFile() (*models.LocalFile) {
	testdata.InitDateTimes()
	return &models.LocalFile{
		Path: "data/metadata.xml",
		Size: 5105,
		Created: testdata.TimeZero,
		Modified: testdata.Apr_25_2014,
		Md5: "84586caa94ff719e93b802720501fcc7",
		Md5Verified: testdata.Apr_25_2014,
		Sha256: "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784",
		Sha256Generated: testdata.June_9_2014,
		Uuid: "b21fdb34-1f79-4101-62c5-56918f4782fc",
		UuidGenerated: testdata.June_9_2014,
		MimeType: "application/xml",
		ErrorMessage: "",
		StorageURL: "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml",
		StoredAt: testdata.July_3_2014,
		StorageMd5: "84586caa94ff719e93b802720501fcc7",
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml",
		IdentifierAssigned: testdata.Apr_25_2014,
		ExistingFile: false,
		NeedsSave: true,
	}
}


func TestToGenericFile(t *testing.T) {
	file := getLocalFile()
	genericFile, _ := file.ToGenericFile()
	expectedIdentifier := "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml"
	if genericFile.Identifier != expectedIdentifier {
		t.Errorf("Identifier expected '%s', got '%s'", expectedIdentifier, genericFile.Identifier)
	}
	expectedFormat := "application/xml"
	if genericFile.Format != expectedFormat {
		t.Errorf("Format expected '%s', got '%s'", expectedFormat, genericFile.Format)
	}
	expectedURI := "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml"
	if genericFile.URI != expectedURI {
		t.Errorf("URI expected '%s', got '%s'", expectedURI, genericFile.URI)
	}
	expectedSize := int64(5105)
	if genericFile.Size != expectedSize {
		t.Errorf("Size expected %d, got %d", expectedSize, genericFile.Size)
	}
	if genericFile.Created != testdata.Apr_25_2014 {
		t.Errorf("Created expected '%v', got '%v'", testdata.Apr_25_2014, genericFile.Created)
	}
	if genericFile.Modified != testdata.Apr_25_2014 {
		t.Errorf("Modified expected '%v', got '%v'", testdata.Apr_25_2014, genericFile.Modified)
	}
	if len(genericFile.ChecksumAttributes) != 2 {
		t.Errorf("GenericFile should have 2 checksum attributes")
	}
	for i := range genericFile.ChecksumAttributes {
		cs := genericFile.ChecksumAttributes[i]
		if i == 0 {
			if cs.Algorithm != "md5" {
				t.Errorf("First algorithm should be md5")
			}
			if cs.DateTime != testdata.Apr_25_2014 {
				t.Errorf("Checksum DateTime should be %v", testdata.Apr_25_2014)
			}
			if cs.Digest != "84586caa94ff719e93b802720501fcc7" {
				t.Errorf("Checksum Digest should be '84586caa94ff719e93b802720501fcc7'")
			}
		} else {
			if cs.Algorithm != "sha256" {
				t.Errorf("First algorithm should be sha256")
			}
			if cs.DateTime != testdata.June_9_2014 {
				t.Errorf("Checksum DateTime should be %v", testdata.June_9_2014)
			}
			if cs.Digest != "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784" {
				t.Errorf("Checksum Digest should be 'ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784'")
			}
		}
	}
	// We'll test individual events below
	if len(genericFile.Events) != 5 {
		t.Errorf("PremisEvents should contain 5 events")
	}
}

func TestPremisEvents(t *testing.T) {
	file := getLocalFile()
	events, _ := file.PremisEvents()
	if len(events) != 5 {
		t.Errorf("PremisEvents() should have returned 5 events")
		return
	}

	// Fixity check event
	event := events[0]
	if event.Identifier == "" {
		t.Errorf("Event.Identifier should not be empty")
	}
	if event.EventType != "fixity_check" {
		t.Errorf("Event.EventType expected 'fixity_check', got '%s'", event.EventType)
	}
	if event.DateTime != file.Md5Verified {
		t.Errorf("Event.DateTime expected '%v', got '%v'", file.Md5Verified, event.DateTime)
	}
	expectedDetail := "Fixity check against registered hash"
	if event.Detail != expectedDetail {
		t.Errorf("Event.Detail expected '%s', got '%s'", expectedDetail, event.Detail)
	}
	expectedOutcome := string(constants.StatusSuccess)
	if event.Outcome != expectedOutcome {
		t.Errorf("Event.Outcome expected '%s', got '%s'", expectedOutcome, event.Outcome)
	}
	expectedOutcomeDetail := fmt.Sprintf("md5:%s", file.Md5)
	if event.OutcomeDetail != expectedOutcomeDetail {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", expectedOutcomeDetail, event.OutcomeDetail)
	}
	if event.Object != "Go crypto/md5" {
		t.Errorf("Event.Object expected 'Go crypto/md5', got '%s'", event.Object)
	}
	expectedAgent := "http://golang.org/pkg/crypto/md5/"
	if event.Agent != expectedAgent {
		t.Errorf("Event.Agent expected '%s', got '%s'", expectedAgent, event.Agent)
	}
	if event.OutcomeInformation != "Fixity matches" {
		t.Errorf("event.OutcomeInformation expected 'Fixity matches', got '%s'", event.OutcomeInformation)
	}

	// Copy to S3 event
	event = events[1]
	if event.EventType != "ingest" {
		t.Errorf("Event.EventType expected 'ingest', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.StorageMd5 {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.StorageMd5, event.OutcomeDetail)
	}

	// Sha256 fixity generation
	event = events[2]
	if event.EventType != "fixity_generation" {
		t.Errorf("Event.EventType expected 'fixity_generation', got '%s'", event.EventType)
	}
	expectedOutcomeDetail = fmt.Sprintf("sha256:%s", file.Sha256)
	if event.OutcomeDetail != expectedOutcomeDetail {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", expectedOutcomeDetail, event.OutcomeDetail)
	}

	// Identifier assignment (friendly identifier)
	event = events[3]
	if event.EventType != "identifier_assignment" {
		t.Errorf("Event.EventType expected 'identifier_assignment', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.Identifier {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.Identifier, event.OutcomeDetail)
	}

	// Identifier assignment (storage URL)
	event = events[4]
	if event.EventType != "identifier_assignment" {
		t.Errorf("Event.EventType expected 'identifier_assignment', got '%s'", event.EventType)
	}
	if event.OutcomeDetail != file.StorageURL {
		t.Errorf("Event.OutcomeDetail expected '%s', got '%s'", file.StorageURL, event.OutcomeDetail)
	}
}

func TestReplicationEvent(t *testing.T) {
	file := getLocalFile()

	url := "https://s3-us-west-2.amazonaws.com/aptrust.test.preservation.oregon/207f95bd-f636-4532-4160-6519292f1bc2"
	event, err := file.ReplicationEvent(url)
	if err != nil {
		t.Error(err)
	}
	if event.OutcomeDetail != url {
		t.Errorf("OutcomeDetail was '%s', but should have been '%s'",
			event.OutcomeDetail, url)
	}
	if event.Outcome != string(constants.StatusSuccess) {
		t.Errorf("Outcome should have been Success, but was %s", event.Outcome)
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Expected UUID for event identifier. Got %s", event.Identifier)
	}
	if event.DateTime.IsZero() {
		t.Errorf("Event DateTime is missing.")
	}

	badUrl := "i am not a url"
	event, err = file.ReplicationEvent(badUrl)
	if err == nil {
		t.Error("File.ReplicationEvent accepted an invalid URL")
	}
}
