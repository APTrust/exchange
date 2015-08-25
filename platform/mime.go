// +build !partners

// This requires an external C library that our partners won't have,
// so this file is not compiled when the flag -tags=partners
package platform

import (
	"fmt"
	"github.com/rakyll/magicmime"
	"regexp"
	"sync"
)

// magicMime is the MimeMagic database. We want
// just one copy of this open at a time.
var magicMime *magicmime.Magic

// We need to restrict access to the underlying MagicMime
// C library, because it sometimes fails or returns nonsense
// (unprintable characters) when accessed by multiple goroutines
// at once. The idiomatic way to do this would be with a
// channel and a goroutine, but the code that calls this
// (in bag.go) is not currently in a state to do that. (That is
// synchronous, single-channel code that includes a lot of
// sequential operations.) So here, we're conrolling access with
// a mutex, which will be blocking. But the calls to MagicMime
// are fast, and this should not be a bottleneck.
var mutex = &sync.Mutex{}

var validMimeType = regexp.MustCompile(`^\w+/\w+$`)

func GuessMimeType(absPath string) (mimeType string, err error) {
	// Open the Mime Magic DB only once.
	if magicMime == nil {
		magicMime, err = magicmime.New(magicmime.MAGIC_MIME_TYPE)
		if err != nil {
			return "", fmt.Errorf("Error opening MimeMagic database: %v", err)
		}
	}

	// Get the mime type of the file. In some cases, MagicMime
	// returns an empty string, and in rare cases (about 1 in 10000),
	// it returns unprintable characters. These are not valid mime
	// types and cause ingest to fail. So we default to the safe
	// application/binary and then set the MimeType only if
	// MagicMime returned something that looks legit.
	mimeType = "application/binary"
	mutex.Lock()
	guessedType, _ := magicMime.TypeByFile(absPath)
	mutex.Unlock()
	if guessedType != "" && validMimeType.MatchString(guessedType) {
		mimeType = guessedType
	}
	return mimeType, nil
}
