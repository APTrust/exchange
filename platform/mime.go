// +build !partners

// Package platform provides build options for different platforms.
// This requires an external C library that our partners won't have,
// so this file is not compiled when the flag -tags=partners
package platform

import (
	"fmt"
	"github.com/rakyll/magicmime"
	"regexp"
	"sync"
)

// IsPartnerBuild will be true when we're building partner tools,
// and false otherwise.
var IsPartnerBuild = false

// decoder is the MimeMagic database. We want
// just one copy of this open at a time.
var decoder *magicmime.Decoder

// We need to restrict access to the underlying MagicMime
// C library, because it sometimes fails or returns nonsense
// (unprintable characters) when accessed by multiple goroutines
// at once.
var mutex = &sync.Mutex{}

// validaMimeType describes what a valid mime type should look like.
var validMimeType = regexp.MustCompile(`^\w+/\w+$`)

// GuessMimeType uses the Mime Magic library to figure out the mime
// type of the file at absPath. If this can't figure out the mime type,
// it returns "application/binary".
func GuessMimeType(absPath string) (mimeType string, err error) {
	// Open the Mime Magic DB only once.
	if decoder == nil {
		decoder, err = magicmime.NewDecoder(magicmime.MAGIC_MIME_TYPE)
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
	guessedType, _ := decoder.TypeByFile(absPath)
	mutex.Unlock()
	if guessedType != "" && validMimeType.MatchString(guessedType) {
		mimeType = guessedType
	}
	return mimeType, nil
}

// GuessMimeTypeByBuffer uses the Mime Magic library to figure out the mime
// type of the file by examining the first N bytes (however long buffer is).
// Use this for very large files, when you don't want GuessMimeType to try
// to read the whole file. Usually, the first few bytes are sufficient for buf.
// If this can't figure out the mime type, it returns "application/binary".
func GuessMimeTypeByBuffer(buf []byte) (mimeType string, err error) {
	// Open the Mime Magic DB only once.
	if decoder == nil {
		decoder, err = magicmime.NewDecoder(magicmime.MAGIC_MIME_TYPE)
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
	guessedType, _ := decoder.TypeByBuffer(buf)
	mutex.Unlock()
	if guessedType != "" && validMimeType.MatchString(guessedType) {
		mimeType = guessedType
	}
	return mimeType, nil
}
