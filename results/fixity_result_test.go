package results_test

import (
	"github.com/APTrust/exchange/results"
	"github.com/APTrust/exchange/models"
	"testing"
	"time"
)

var md5sum = "1234567890"
var sha256sum = "fedcba9876543210"

func getGenericFile() (*models.GenericFile) {
	checksums := make([]*models.ChecksumAttribute, 2)
	checksums[0] = &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: md5sum,
	}
	checksums[1] = &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: sha256sum,
	}
	return &models.GenericFile{
		URI: "https://s3.amazonaws.com/aptrust.preservation.storage/52a928da-89ef-48c6-4627-826d1858349b",
		ChecksumAttributes: checksums,
	}
}

func TestBucketAndKey(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	bucket, key, err := result.BucketAndKey()
	if err != nil {
		t.Errorf("BucketAndKey() returned error: %v", err)
	}
	if bucket != "aptrust.preservation.storage" {
		t.Errorf("BucketAndKey() returned bucket name '%s', expected 'aptrust.preservation.storage'", bucket)
	}
	if key != "52a928da-89ef-48c6-4627-826d1858349b" {
		t.Errorf("BucketAndKey() returned key '%s', expected '52a928da-89ef-48c6-4627-826d1858349b'", key)
	}
}

func TestBucketAndKeyWithBadUri(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	result.GenericFile.URI = "http://example.com"
	_, _, err := result.BucketAndKey()
	if err == nil {
		t.Errorf("BucketAndKey() should have returned an error for invalid URI")
		return
	}
	if result.Result.Errors[0] != "GenericFile URI 'http://example.com' is invalid" {
		t.Errorf("BucketAndKey() did not set descriptive error message for bad URI")
	}
	if result.Result.Retry == true {
		t.Errorf("Retry should have been set to false after fatal error.")
	}
}


func TestSha256Matches(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	matches, err := result.Sha256Matches()
	if err != nil {
		t.Error(err)
	}
	if matches == false {
		t.Errorf("Sha256Matches should have returned true")
	}


	result.Sha256 = "some random string"
	matches, err = result.Sha256Matches()
	if err != nil {
		t.Error(err)
	}
	if matches == true {
		t.Errorf("Sha256Matches should have returned false")
	}
}

func TestMissingChecksums(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	_, err := result.Sha256Matches()
	if err == nil {
		t.Errorf("Sha256Matches should have returned a usage error")
	}

	result.Sha256 = sha256sum
	result.GenericFile.ChecksumAttributes = make([]*models.ChecksumAttribute, 2)
	_, err = result.Sha256Matches()
	if err == nil {
		t.Errorf("Sha256Matches should have returned a usage error")
	}
}

func TestGotDigestFromPreservationFile(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	if result.GotDigestFromPreservationFile() == true {
		t.Errorf("GotDigestFromPreservationFile() should have returned false")
	}
	result.Sha256 = sha256sum
	if result.GotDigestFromPreservationFile() == false {
		t.Errorf("GotDigestFromPreservationFile() should have returned true")
	}
}

func TestGenericFileHasDigest(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	if result.GenericFileHasDigest() == false {
		t.Errorf("GenericFileHasDigest() should have returned true")
	}
	// Make the SHA256 checksum disappear
	for i := range result.GenericFile.ChecksumAttributes {
		result.GenericFile.ChecksumAttributes[i].Algorithm = "Md five and a half"
	}
	if result.GenericFileHasDigest() == true {
		t.Errorf("GenericFileHasDigest() should have returned false")
	}
}

func TestFedoraSha256(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	if result.FedoraSha256() != sha256sum {
		t.Errorf("FedoraSha256() should have returned", sha256sum)
	}
}

func TestFixityCheckPossible(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	if result.FixityCheckPossible() == false {
		t.Errorf("FixityCheckPossible() should have returned true")
	}
	result.Sha256 = ""
	if result.FixityCheckPossible() == true {
		t.Errorf("FixityCheckPossible() should have returned false")
	}
	result.Sha256 = sha256sum
	// Make the SHA256 checksum disappear
	for i := range result.GenericFile.ChecksumAttributes {
		result.GenericFile.ChecksumAttributes[i].Algorithm = "Md five and a half"
	}
	if result.FixityCheckPossible() == true {
		t.Errorf("FixityCheckPossible() should have returned false")
	}
}


func TestBuildPremisEvent_Success(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	premisEvent, err := result.BuildPremisEvent()
	if err != nil {
		t.Errorf("BuildPremisEvent() returned an error: %v", err)
	}
	if len(premisEvent.Identifier) != 36 {
		t.Errorf("PremisEvent.Identifier '%s' is not a valid UUID", premisEvent.Identifier)
	}
	if premisEvent.EventType != "fixity_check" {
		t.Errorf("PremisEvent.EventType '%s' should be 'fixity_check'", premisEvent.EventType)
	}
	if time.Now().Unix() - premisEvent.DateTime.Unix() > 5 {
		t.Errorf("PremisEvent.DateTime should be close to current time, but it's not.")
	}
	if premisEvent.Detail != "Fixity check against registered hash" {
		t.Errorf("Unexpected PremisEvent.Detail '%s'", premisEvent.Detail)
	}
	if premisEvent.Outcome != "success" {
		t.Errorf("PremisEvent.Outcome expected 'success' but got '%s'", premisEvent.Outcome)
	}
	if premisEvent.OutcomeDetail != sha256sum {
		t.Errorf("PremisEvent.OutcomeDetail expected '%s' but got '%s'",
			sha256sum, premisEvent.OutcomeDetail)
	}
	if premisEvent.Object != "Go language cryptohash" {
		t.Errorf("PremisEvent.Outcome expected 'Go language cryptohash' but got '%s'",
			premisEvent.Object)
	}
	if premisEvent.Agent != "http://golang.org/pkg/crypto/sha256/" {
		t.Errorf("PremisEvent.Outcome expected 'http://golang.org/pkg/crypto/sha256/' but got '%s'",
			premisEvent.Agent)
	}
	if premisEvent.OutcomeInformation != "Fixity matches" {
		t.Errorf("PremisEvent.OutcomeInformation expected 'Fixity matches' but got '%s'",
			premisEvent.OutcomeInformation)
	}
}

func TestBuildPremisEvent_Failure(t *testing.T) {
	result := results.NewFixityResult(getGenericFile())
	result.Sha256 = "xxx-xxx-xxx"
	premisEvent, err := result.BuildPremisEvent()
	if err != nil {
		t.Errorf("BuildPremisEvent() returned an error: %v", err)
	}
	if len(premisEvent.Identifier) != 36 {
		t.Errorf("PremisEvent.Identifier '%s' is not a valid UUID", premisEvent.Identifier)
	}
	if premisEvent.EventType != "fixity_check" {
		t.Errorf("PremisEvent.EventType '%s' should be 'fixity_check'", premisEvent.EventType)
	}
	if time.Now().Unix() - premisEvent.DateTime.Unix() > 5 {
		t.Errorf("PremisEvent.DateTime should be close to current time, but it's not.")
	}
	if premisEvent.Detail != "Fixity does not match expected value" {
		t.Errorf("Unexpected PremisEvent.Detail '%s'", premisEvent.Detail)
	}
	if premisEvent.Outcome != "failure" {
		t.Errorf("PremisEvent.Outcome expected 'failure' but got '%s'", premisEvent.Outcome)
	}
	if premisEvent.OutcomeDetail != result.Sha256 {
		t.Errorf("PremisEvent.OutcomeDetail expected '%s' but got '%s'",
			sha256sum, premisEvent.OutcomeDetail)
	}
	if premisEvent.Object != "Go language cryptohash" {
		t.Errorf("PremisEvent.Outcome expected 'Go language cryptohash' but got '%s'",
			premisEvent.Object)
	}
	if premisEvent.Agent != "http://golang.org/pkg/crypto/sha256/" {
		t.Errorf("PremisEvent.Outcome expected 'http://golang.org/pkg/crypto/sha256/' but got '%s'",
			premisEvent.Agent)
	}
	if premisEvent.OutcomeInformation != "Expected digest 'fedcba9876543210', got 'xxx-xxx-xxx'" {
		t.Errorf("PremisEvent.OutcomeInformation expected '%s' but got '%s'",
			result.Result.Errors[0], premisEvent.OutcomeInformation)
	}
}
