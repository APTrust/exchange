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
