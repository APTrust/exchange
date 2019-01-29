package util

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"path"
	"regexp"
	"strings"
)

var reManifest *regexp.Regexp = regexp.MustCompile("^manifest-[A-Za-z0-9]+\\.txt$")
var reTagManifest *regexp.Regexp = regexp.MustCompile("^tagmanifest-[A-Za-z0-9]+\\.txt$")
var reLegal *regexp.Regexp = regexp.MustCompile("^[A-Za-z0-9\\-_\\.]+$")

// OwnerOf returns the domain name of the institution that owns the
// specified bucket. For example, if bucketName is 'aptrust.receiving.unc.edu'
// the return value will be 'unc.edu'.
func OwnerOf(bucketName string) (institution string) {
	if bucketName == constants.ReceiveTestBucketPrefix {
		// Actual test.edu receiving bucket for production.
		// Didn't anticipate this case back in 2014. Oops.
		institution = "test.edu"
	} else if strings.HasPrefix(bucketName, constants.ReceiveTestBucketPrefix) {
		institution = strings.Replace(bucketName, constants.ReceiveTestBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, constants.ReceiveBucketPrefix) {
		institution = strings.Replace(bucketName, constants.ReceiveBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, constants.RestoreBucketPrefix) {
		institution = strings.Replace(bucketName, constants.RestoreBucketPrefix, "", 1)
	} else if bucketName == "aptrust.test.receiving" || bucketName == "aptrust.integration.test" {
		// Special case for integration tests
		institution = "test.edu"
	}
	return institution
}

// RestorationBucketFor returns the name of the specified institution's
// restoration bucket. E.g. institution 'unc.edu' returns bucketName
// 'aptrust.restore.unc.edu'. The second param, restoreToTestBuckets,
// is set the in the config json file and accessible through
// Config.RestoreToTestBuckets. It should be true everywhere
// except in the production environment. If true, this will return
// 'aptrust.restore.test.unc.edu'.
func RestorationBucketFor(institution string, restoreToTestBuckets bool) (bucketName string) {
	if restoreToTestBuckets {
		return constants.RestoreBucketPrefix + "test." + institution
	}
	return constants.RestoreBucketPrefix + institution
}

// BagNameFromTarFileName returns the bag name of the specified tar file.
// This works even for tar files with names like 'test.edu.my_bag.b01.of12.tar'.
// That will return 'test.edu.my_bag'.
func BagNameFromTarFileName(pathToTarFile string) string {
	fileName := path.Base(pathToTarFile)
	return CleanBagName(fileName)
}

// CleanBagName returns the clean bag name. That's the tar file name minus
// the tar extension and any ".bagN.ofN" suffix.
func CleanBagName(bagName string) string {
	// Strip the .tar suffix
	nameWithoutTar := bagName
	if strings.HasSuffix(bagName, ".tar") {
		nameWithoutTar = bagName[0 : len(bagName)-4]
	}
	// Now get rid of the .b001.of200 suffix if this is a multi-part bag.
	cleanName := constants.MultipartSuffix.ReplaceAll([]byte(nameWithoutTar), []byte(""))
	return string(cleanName)
}

// Min returns the minimum of x or y. The Math package has this function
// but you have to cast to floats.
func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// Base64EncodeMd5 returns a base64-encoded md5 digest. The is the format S3 wants.
func Base64EncodeMd5(md5Digest string) (string, error) {
	// We'll get error if md5 contains non-hex characters. Catch
	// that below, when S3 tells us our md5 sum is invalid.
	md5Bytes, err := hex.DecodeString(md5Digest)
	if err != nil {
		detailedError := fmt.Errorf("Md5 sum '%s' contains invalid characters.",
			md5Digest)
		return "", detailedError
	}
	// Base64-encoded md5 sum suitable for sending to S3
	base64md5 := base64.StdEncoding.EncodeToString(md5Bytes)
	return base64md5, nil
}

// LooksLikeURL returns true if url looks like a URL.
func LooksLikeURL(url string) bool {
	reUrl := regexp.MustCompile(`^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$`)
	return reUrl.Match([]byte(url))
}

// LooksLikeUUID returns true if uuid looks like a valid UUID.
func LooksLikeUUID(uuid string) bool {
	reUUID := regexp.MustCompile(`(?i)^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}?)$`)
	return reUUID.Match([]byte(uuid))
}

// CleanString cleans a string we might find a config file, trimming leading
// and trailing spaces, single quotes and double quoted. Note that
// leading and trailing spaces inside the quotes are not trimmed.
func CleanString(str string) string {
	cleanStr := strings.TrimSpace(str)
	// Strip leading and traling quotes, but only if string has matching
	// quotes at both ends.
	if strings.HasPrefix(cleanStr, "'") && strings.HasSuffix(cleanStr, "'") ||
		strings.HasPrefix(cleanStr, "\"") && strings.HasSuffix(cleanStr, "\"") {
		return cleanStr[1 : len(cleanStr)-1]
	}
	return cleanStr
}

// BucketNameAndKey returns the bucket name and key of the specified
// url, which is expected to be an S3 URI.
func BucketNameAndKey(uri string) (string, string) {
	relativeUri := strings.Replace(uri, constants.S3UriPrefix, "", 1)
	parts := strings.SplitN(relativeUri, "/", 2)
	return parts[0], parts[1]
}

// GetInstitutionFromBagName returns the instution name from the bag name,
// or an error if the bag name does not contain the institution name.
// For example, "virginia.edu.bag_of_videos.tar" returns "virginia.edu" and no
// errors. "virginia.edu.bag_of_videos" returns the same thing.
// But "bag_of_videos.tar" or "virginia.bag_of_videos.tar" returns
// an error because the institution identifier is missing from
// the bag name.
func GetInstitutionFromBagName(bagName string) (string, error) {
	parts := strings.Split(bagName, ".")
	if len(parts) < 3 {
		message := fmt.Sprintf(
			"Bag name '%s' should start with your institution ID,\n"+
				"followed by a period and the object name.\n"+
				"For example, 'miami.my_archive.tar' for a tar file,\n"+
				"or 'miami.my_archive' for a directory.",
			bagName)
		return "", fmt.Errorf(message)
	}
	if len(parts) > 3 && (parts[1] == "edu" || parts[1] == "org") {
		return fmt.Sprintf("%s.%s", parts[0], parts[1]), nil
	}
	return parts[0], nil
}

// HasSavableNamereturns true if the file name indicates this is something we
// should save to long-term storage. As of late March, 2016, we save everything
// in the bag except bagit.txt, manifest-<algo>.txt and
// tagmanifest-<algo>.txt. Those files we don't save will be reconstructed
// when the bag is restored.
//
// Param filename should be the relative path of the file within the bag.
// For example, "tagmanifest-sha256.txt" or "data/images/photo_01.jpg".
// This is important, because a file called "manifest-md5.txt" will return
// false (indicating it should not be saved), while a file called
// "data/manifest-md5.txt" will return true, because its file path indicates
// it's part of the payload.
//
// We reconstruct bagit.txt because we may have moved to a newer version
// by the time the file is restored. We reconstruct manifests and tag
// manifests because payload files and tag files may be deleted or
// overwritten by the depositor between initial ingest and restoration.
//
// And did you know both savable and saveable are correct? I chose the
// former to reduce the size of our compiled binary by one byte. That
// could save us pennies over the next 10,000 years.
func HasSavableName(filename string) bool {
	return !(filename == "." ||
		filename == ".." ||
		filename == "bagit.txt" ||
		strings.HasPrefix(filename, "._") || // mac junk files
		strings.Contains(filename, "/._") || // mac junk files
		reTagManifest.MatchString(filename) ||
		reManifest.MatchString(filename))
}

// LooksLikeJunkFile returns true if the filename looks like a
// MacOS junk file. These are dot-underscore files that contain
// extended file attributes. These files are often unintentionally
// included in tarred bags created on MacOS. They may not appear in
// the manifests, and that will cause bag validation to fail.
// Part of https://www.pivotaltracker.com/story/show/151265762
func LooksLikeJunkFile(filename string) bool {
	return (strings.HasPrefix(filename, "._") ||
		strings.Contains(filename, "/._"))
}

// StringListContains returns true if the list of strings contains item.
func StringListContains(list []string, item string) bool {
	if list != nil {
		for i := range list {
			if list[i] == item {
				return true
			}
		}
	}
	return false
}

// IntListContains returns true if the list of ints contains item.
func IntListContains(list []int, item int) bool {
	if list != nil {
		for i := range list {
			if list[i] == item {
				return true
			}
		}
	}
	return false
}

// PointerToString returns the string pointed to, or an empty
// string if the pointer is nil.
func PointerToString(strPointer *string) string {
	if strPointer == nil {
		return ""
	}
	return *strPointer
}

// DeleteFromStringList deletes item from list and returns a new slice.
func DeleteFromStringList(list []string, item string) []string {
	newList := make([]string, 0)
	if list != nil {
		for i := range list {
			if list[i] != item {
				newList = append(newList, list[i])
			}
		}
	}
	return newList
}
