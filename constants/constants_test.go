package constants_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/stretchr/testify/assert"
	"testing"
)

var errShouldMatch = "Regex does not match valid pattern"
var errShouldNotMatch = "Regex matches invalid pattern"

// This is tested more thoroughly elsewhere.
func TestMultipartSuffix(t *testing.T) {
	pattern := constants.MultipartSuffix
	assert.True(t, pattern.MatchString("bag.b02.of04"), errShouldMatch)
	assert.False(t, pattern.MatchString("bag.bag02of04"), errShouldNotMatch)
}

func TestAPTrustFileNamePattern(t *testing.T) {
	pattern := constants.APTrustFileNamePattern
	assert.True(t, pattern.MatchString("file_NAm3.is-valid"), errShouldMatch)
	assert.True(t, pattern.MatchString(".file_NAm3.is-valid"), errShouldMatch)
	assert.True(t, pattern.MatchString("File%2fWith%2fEscaped%2fSlashes"), errShouldMatch)

	assert.False(t, pattern.MatchString("-not-valid-begins.with.dash"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("not-valid+contains-plus"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("not-valid(parens)"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("\"not-valid{'}@all!"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("$nope*"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("^negatory"), errShouldNotMatch)
}

func TestPosixFileNamePattern(t *testing.T) {
	pattern := constants.PosixFileNamePattern
	assert.True(t, pattern.MatchString("file_NAm3.is-valid"), errShouldMatch)
	assert.True(t, pattern.MatchString(".file_NAm3.is-valid"), errShouldMatch)

	// Differences from APTrust pattern:
	// OK for POSIX to start with dash...
	assert.True(t, pattern.MatchString("-not-valid-begins.with.dash"), errShouldNotMatch)
	// ...but not to contain percent signs.
	assert.False(t, pattern.MatchString("File%2fWith%2fEscaped%2fSlashes"), errShouldNotMatch)

	assert.False(t, pattern.MatchString("not-valid+contains-plus"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("not-valid(parens)"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("\"not-valid{'}@all!"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("$nope*"), errShouldNotMatch)
	assert.False(t, pattern.MatchString("^negatory"), errShouldNotMatch)

}
