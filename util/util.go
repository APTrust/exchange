package util

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"regexp"
	"strings"
)

// Returns the domain name of the institution that owns the specified bucket.
// For example, if bucketName is 'aptrust.receiving.unc.edu' the return value
// will be 'unc.edu'.
func OwnerOf(bucketName string) (institution string) {
	if strings.HasPrefix(bucketName, constants.ReceiveTestBucketPrefix) {
		institution = strings.Replace(bucketName, constants.ReceiveTestBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, constants.ReceiveBucketPrefix) {
		institution = strings.Replace(bucketName, constants.ReceiveBucketPrefix, "", 1)
	} else if strings.HasPrefix(bucketName, constants.RestoreBucketPrefix) {
		institution = strings.Replace(bucketName, constants.RestoreBucketPrefix, "", 1)
	}
	return institution
}

// Returns the name of the specified institution's restoration bucket.
// E.g. institution 'unc.edu' returns bucketName 'aptrust.restore.unc.edu'
func RestorationBucketFor(institution string) (bucketName string) {
	return constants.RestoreBucketPrefix + institution
}

// Given the name of a tar file, returns the clean bag name. That's
// the tar file name minus the tar extension and any ".bagN.ofN" suffix.
func CleanBagName(bagName string) (string, error) {
	if len(bagName) < 5 {
		return "", fmt.Errorf("'%s' is not a valid tar file name", bagName)
	}
	// Strip the .tar suffix
	nameWithoutTar := bagName[0:len(bagName)-4]
	// Now get rid of the .b001.of200 suffix if this is a multi-part bag.
	cleanName := constants.MultipartSuffix.ReplaceAll([]byte(nameWithoutTar), []byte(""))
	return string(cleanName), nil
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

// Returns a base64-encoded md5 digest. The is the format S3 wants.
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

// Returns true if url looks like a URL.
func LooksLikeURL(url string) (bool) {
	reUrl := regexp.MustCompile(`^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$`)
	return reUrl.Match([]byte(url))
}

func LooksLikeUUID(uuid string) (bool) {
	reUUID := regexp.MustCompile(`(?i)^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}?)$`)
	return reUUID.Match([]byte(uuid))
}

// Cleans a string we might find a config file, trimming leading
// and trailing spaces, single quotes and double quoted. Note that
// leading and trailing spaces inside the quotes are not trimmed.
func CleanString(str string) (string) {
	cleanStr := strings.TrimSpace(str)
	// Strip leading and traling quotes, but only if string has matching
	// quotes at both ends.
	if strings.HasPrefix(cleanStr, "'") && strings.HasSuffix(cleanStr, "'") ||
		strings.HasPrefix(cleanStr, "\"") && strings.HasSuffix(cleanStr, "\"") {
		return cleanStr[1:len(cleanStr) - 1]
	}
	return cleanStr
}

// Given an S3 URI, returns the bucket name and key.
func BucketNameAndKey(uri string) (string, string) {
	relativeUri := strings.Replace(uri, constants.S3UriPrefix, "", 1)
	parts := strings.SplitN(relativeUri, "/", 2)
	return parts[0], parts[1]
}

// Returns the instution name from the bag name, or an error if
// the bag name does not contain the institution name. For example,
// "virginia.edu.bag_of_videos.tar" returns "virginia.edu" and no
// errors. "virginia.edu.bag_of_videos" returns the same thing.
// But "bag_of_videos.tar" or "virginia.bag_of_videos.tar" returns
// an error because the institution identifier is missing from
// the bag name.
func GetInstitutionFromBagName(bagName string) (string, error) {
	parts := strings.Split(bagName, ".")
	if len(parts) < 3 {
		message := fmt.Sprintf(
			"Bag name '%s' should start with your institution ID,\n" +
				"followed by a period and the object name.\n" +
				"For example, 'miami.my_archive.tar' for a tar file,\n" +
				"or 'miami.my_archive' for a directory.",
			bagName)
		return "", fmt.Errorf(message)
	}
	if len(parts) > 3 && (parts[1] == "edu" || parts[1] == "org") {
		return fmt.Sprintf("%s.%s", parts[0], parts[1]), nil
	}
	return parts[0], nil
}
