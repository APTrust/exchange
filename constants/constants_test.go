package constants_test

import (
	"github.com/APTrust/exchange/constants"
	"testing"
)


// This is tested more thoroughly elsewhere.
func TestMultipartSuffix(t *testing.T) {
	if !constants.MultipartSuffix.MatchString("bag.b02.of04") {
		t.Errorf("Regex does not match valid pattern")
	}
	if constants.MultipartSuffix.MatchString("bag.bag02of04") {
		t.Errorf("Regex matches invalid pattern")
	}
}
