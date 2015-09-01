package result_test

import (
	"github.com/APTrust/exchange/result"
	"testing"
)

func TestTagValue(t *testing.T) {
	bagReadResult := &result.BagReadResult{}
	bagReadResult.Tags = make([]result.Tag, 2)
	bagReadResult.Tags[0] = result.Tag{Label: "Label One", Value: "Value One"}
	bagReadResult.Tags[1] = result.Tag{Label: "Label Two", Value: "Value Two"}

	if bagReadResult.TagValue("LABEL ONE") != "Value One" {
		t.Error("TagValue returned wrong result.")
	}
	if bagReadResult.TagValue("Label Two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if bagReadResult.TagValue("label two") != "Value Two" {
		t.Error("TagValue returned wrong result.")
	}
	if bagReadResult.TagValue("Non-existent label") != "" {
		t.Error("TagValue returned wrong result.")
	}
}

func TestAccess(t *testing.T) {
	bagReadResult := &result.BagReadResult{}
	bagReadResult.Tags = make([]result.Tag, 1)
	bagReadResult.Tags[0] = result.Tag{
		Label: "Access",
		Value: "consortia",
	}

	if bagReadResult.Access() != "consortia" {
		t.Error("Access() should have returned consortia")
	}

	// Test reading of deprecated Rights tag.
	bagReadResult.Tags[0] = result.Tag{
		Label: "Rights",
		Value: "institution",
	}
	if bagReadResult.Access() != "institution" {
		t.Error("Access() should have returned institution")
	}

	bagReadResult.Tags[0] = result.Tag{Label: "X", Value: "Y"}
	if bagReadResult.Access() != "" {
		t.Error("Access() should have returned empty string")
	}
}

func TestTitle(t *testing.T) {
	bagReadResult := &result.BagReadResult{}
	bagReadResult.Tags = make([]result.Tag, 1)
	bagReadResult.Tags[0] = result.Tag{
		Label: "Title",
		Value: "Hang on Sloopy",
	}

	if bagReadResult.Title() != "Hang on Sloopy" {
		t.Error("Title() should have returned an old song title")
	}

	bagReadResult.Tags[0] = result.Tag{Label: "X", Value: "Y"}
	if bagReadResult.Title() != "" {
		t.Error("Title() is just making stuff up")
	}
}

func TestDescription(t *testing.T) {
	bagReadResult := &result.BagReadResult{}
	bagReadResult.Tags = make([]result.Tag, 1)
	bagReadResult.Tags[0] = result.Tag{
		Label: "Internal-Sender-Description",
		Value: "Loud and obtuse",
	}

	if bagReadResult.Description() != "Loud and obtuse" {
		t.Error("Description() should have returned 'Loud and obtuse'")
	}

	bagReadResult.Tags[0] = result.Tag{Label: "X", Value: "Y"}
	if bagReadResult.Description() != "" {
		t.Error("Description() is just making stuff up")
	}
}

func TestAltId(t *testing.T) {
	bagReadResult := &result.BagReadResult{}
	bagReadResult.Tags = make([]result.Tag, 1)
	bagReadResult.Tags[0] = result.Tag{
		Label: "Internal-Sender-Identifier",
		Value: "abc-123",
	}
	expected := make([]string, 1)
	expected[0] = "abc-123"
	if bagReadResult.AltId()[0] != "abc-123" {
		t.Error("AltId() should have returned 'abc-123'")
	}

	bagReadResult.Tags[0] = result.Tag{Label: "X", Value: "Y"}
	if bagReadResult.AltId()[0] != "" {
		t.Error("AltId() is just making stuff up")
	}
}
