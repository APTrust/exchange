package network

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Constants
const (
	// A Gigabyte!
	GIGABYTE int64 = int64(1024 * 1024 * 1024)

	// Files over 5GB in size must be uploaded via multi-part put.
	S3_LARGE_FILE int64 = int64(5 * GIGABYTE)

	// Chunk size for multipart puts to S3: ~500 MB
	S3_CHUNK_SIZE = int64(500000000)
)

type S3Client struct {
	S3 *s3.S3
}

// Returns an S3Client for the specified region, using
// AWS credentials from the environment. Please keep your AWS
// keys out of the source code repos! Store them somewhere
// else and load them into environment variables AWS_ACCESS_KEY_ID
// and AWS_SECRET_ACCESS_KEY.
func NewS3Client(region aws.Region) (*S3Client, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	s3Client := s3.New(auth, region)
	return &S3Client{S3: s3Client}, nil
}

// Returns an S3 client from specific auth credentials,
// instead of reading credentials from the environment.
func NewS3ClientExplicitAuth(region aws.Region, accessKey, secretKey string) (*S3Client, error) {
	auth := aws.Auth {
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	s3Client := s3.New(auth, region)
	return &S3Client{S3: s3Client}, nil
}

// Returns a list of keys in the specified bucket.
// If limit is zero, this will return all the keys in the bucket;
// otherwise, it will return only the number of keys specifed.
// Note that listing all keys may result in the underlying client
// issuing multiple requests.
func (client *S3Client) ListBucket(bucketName string, limit int) (keys []s3.Key, err error) {
	bucket := client.S3.Bucket(bucketName)
	if bucket == nil {
		err = fmt.Errorf("Cannot retrieve bucket: %s", bucketName)
		return nil, err
	}
	actualLimit := limit
	if limit == 0 {
		actualLimit = 1000
	}
	bucketList, err := bucket.List("", "/", "", actualLimit)
	if err != nil {
		return nil, err
	}
	contents := bucketList.Contents
	if len(contents) == 0 {
		return contents, nil
	}
	for limit == 0 {
		lastKey := contents[len(contents)-1].Key
		bucketList, err := bucket.List("", "/", lastKey, actualLimit)
		if err != nil {
			return nil, err
		}
		contents = append(contents, bucketList.Contents...)
		if !bucketList.IsTruncated {
			break
		}
	}
	return contents, nil
}

// Fetches a file from S3 and records info about the download
// in the S3Download object. See S3Download for options.
// If an error occurs, it will be recorded in s3Download.ErrorMessage.
func (client *S3Client) Fetch(s3Download *S3Download) {

	// Get the S3 key, because the S3Download object will
	// want to know the ETag, size, mod date, etc. of this
	// file.
	var err error = nil
	s3Download.S3Key, err = client.GetKey(s3Download.BucketName, s3Download.KeyName)
	if err != nil {
		s3Download.ErrorMessage = err.Error()
		return
	}

	// Get a reader to read from this bucket.
	bucket := client.S3.Bucket(s3Download.BucketName)
	readCloser, err := bucket.GetReader(s3Download.KeyName)
	if err != nil {
		s3Download.ErrorMessage = err.Error()
		return
	}
	defer readCloser.Close()

	// Create the download directory and open a file for writing.
	writers := make([]io.Writer, 0)
	if s3Download.LocalPath == os.DevNull {
		writers = append(writers, ioutil.Discard)
	} else {
		err = os.MkdirAll(filepath.Dir(s3Download.LocalPath), 0755)
		if err != nil {
			s3Download.ErrorMessage = err.Error()
			return
		}
		outputFile, err := os.Create(s3Download.LocalPath)
		if err != nil {
			s3Download.ErrorMessage = err.Error()
			return
		}
		writers = append(writers, outputFile)
		defer outputFile.Close()
	}

	// Create a writer to write the contents to the file,
	// and optionally to pass the bitstream through the
	// md5 and sha256 algorithms while we're at it.
	var multiWriter io.Writer
	var md5Hash hash.Hash
	var sha256Hash hash.Hash
	if s3Download.CalculateMd5 {
		md5Hash = md5.New()
		writers = append(writers, md5Hash)
	}
	if s3Download.CalculateSha256 {
		sha256Hash = sha256.New()
		writers = append(writers, sha256Hash)
	}
	multiWriter = io.MultiWriter(writers...)

	// Copy the file, with several tries. On larger files,
	// we often get a "connection reset by peer" error.
	// Better to retry a few times now than throw this
	// back into the work queue.
	bytesWritten := int64(0)
	for attemptNumber := 0; attemptNumber < 5; attemptNumber++ {
		bytesWritten, err = io.Copy(multiWriter, readCloser)
		if err == nil {
			break
		}
	}
	s3Download.BytesCopied = bytesWritten
	if err != nil {
		s3Download.ErrorMessage = err.Error()
		return
	}

	// Set the checksums, if needed...
	if s3Download.CalculateMd5 {
		s3Download.Md5Digest = fmt.Sprintf("%x", md5Hash.Sum(nil))
	}
	if s3Download.CalculateSha256 {
		s3Download.Sha256Digest = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	}
}

// Returns an S3 key object for the specified file in the
// specified bucket. The key object has the ETag, last mod
// date, size and other useful info.
func (client *S3Client) GetKey(bucketName, fileName string) (*s3.Key, error) {
	bucket := client.S3.Bucket(bucketName)
	listResp, err := bucket.List(fileName, "", "", 1)
	if err != nil {
		err = fmt.Errorf("Error checking key '%s' in bucket '%s': '%v'",
			fileName, bucketName, err)
		return nil, err
	}
	if listResp == nil || len(listResp.Contents) < 1 {
		err = fmt.Errorf("Key '%s' not found in bucket '%s'",
			fileName, bucketName)
		return nil, err
	}
	return &listResp.Contents[0], nil
}


/*

// ===============================================================

// Fetches the specified S3 URL and saves it in the specified localPath.
// Ensures that the directory containing localPath exists, and calculates
// an md5 checksum on download. The FetchResult will tell you whether the
// md5 matched what AWS said it should be. You'll get an error if url is
// not an S3 url, or if it doesn't exist. Check FetchResult.ErrorMessage.
func (client *S3Client) FetchURLToFile(url, localPath string) (*FetchResult) {
	bucketName, key := BucketNameAndKey(url)
	s3Key, err := client.GetKey(bucketName, key)
	if err != nil {
		errMsg := fmt.Sprintf("Could not get key info for %s: %v", url, err)
		return &FetchResult {
			ErrorMessage: errMsg,
		}
	}
	return client.FetchToFile(bucketName, *s3Key, localPath)
}

// Collects info about all of the buckets listed in buckets.
// TODO: Write unit test
func (client *S3Client) CheckAllBuckets(buckets []string) (bucketSummaries []*BucketSummary, errors []error) {
	bucketSummaries = make([]*BucketSummary, 0)
	errors = make([]error, 0)
	for _, bucketName := range buckets {
		bucketSummary, err := client.CheckBucket(bucketName)
		if err != nil {
			errors = append(errors, fmt.Errorf("%s: %v", bucketName, err))
		} else {
			bucketSummaries = append(bucketSummaries, bucketSummary)
		}
	}
	return bucketSummaries, errors
}

// Returns info about the contents of the bucket named bucketName.
// BucketSummary contains the bucket name, a list of keys, and the
// size of the largest file in the bucket.
// TODO: Write unit test
func (client *S3Client) CheckBucket(bucketName string) (bucketSummary *BucketSummary, err error) {
	bucket := client.S3.Bucket(bucketName)
	if bucket == nil {
		err = fmt.Errorf("Cannot retrieve bucket: %s", bucketName)
		return nil, err
	}
	bucketSummary = new(BucketSummary)
	bucketSummary.BucketName = bucketName
	bucketSummary.Keys, err = client.ListBucket(bucketName, 0)
	if err != nil {
		return nil, err
	}
	return bucketSummary, nil
}

// Creates an options struct that adds metadata headers to the S3 put.
func (client *S3Client) MakeOptions(md5sum string, metadata map[string][]string) s3.Options {
	if md5sum != "" {
		return s3.Options{
			ContentMD5: md5sum,
			Meta:       metadata,
		}
	} else {
		return s3.Options{
			Meta: metadata,
		}
	}
}

// Saves a file to S3 with default access of Private.
// The underlying S3 client does not return the md5 checksum
// from s3, but we already have this info elsewhere. If the
// PUT produces no error, we assume the copy worked and the
// files md5 sum is the same on S3 as here.
func (client *S3Client) SaveToS3(bucketName, fileName, contentType string, reader io.Reader, byteCount int64, options s3.Options) (url string, err error) {
	bucket := client.S3.Bucket(bucketName)
	putErr := bucket.PutReader(fileName, reader, byteCount,
		contentType, s3.Private, options)
	if putErr != nil {
		err = fmt.Errorf("Error saving file '%s' to bucket '%s': %v",
			fileName, bucketName, putErr)
		return "", err
	}
	url = fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, fileName)
	return url, nil
}

// Deletes an item from S3
func (client *S3Client) Delete(bucketName, fileName string) error {
	bucket := client.S3.Bucket(bucketName)
	return bucket.Del(fileName)
}

// Sends a large file (>= 5GB) to S3 in 200MB chunks. This operation
// may take several minutes to complete. Note that os.File satisfies
// the s3.ReaderAtSeeker interface.
func (client *S3Client) SaveLargeFileToS3(bucketName, fileName, contentType string,
	reader s3.ReaderAtSeeker, byteCount int64, options s3.Options, chunkSize int64) (url string, err error) {

	bucket := client.S3.Bucket(bucketName)
	multipartPut, err := bucket.InitMulti(fileName, contentType, s3.Private, options)
	if err != nil {
		return "", err
	}

	// Send all of the individual parts to S3 in chunks
	parts, err := multipartPut.PutAll(reader, chunkSize)
	if err != nil {
		abortErr := multipartPut.Abort()
		if abortErr != nil {
			return "", fmt.Errorf("Multipart put failed with error %v "+
				"while uploading a part and abort failed with error %v. "+
				"YOU WILL BE CHARGED FOR THESE FILE PARTS UNTIL YOU DELETE THEM! "+
				"Use multi.ListMulti in the S3 package to list orphaned parts.",
				err, abortErr)
		}
		return "", err
	}

	// This command tells S3 to stitch all the parts into a single file.
	err = multipartPut.Complete(parts)
	if err != nil {
		abortErr := multipartPut.Abort()
		if abortErr != nil {
			return "", fmt.Errorf("Multipart put failed in 'complete' stage "+
				"with error %v and abort failed with error %v",
				err, abortErr)
		}
		return "", err
	}

	resp, err := bucket.Head(fileName, nil)
	if err != nil {
		return "", fmt.Errorf("Files were uploaded to S3, but attempt to "+
			"confirm metadata returned this error: %v", err)
	}

	// Make sure all the meta data made it there.
	// Var metadata is the metadata we sent to S3.
	metadata := options.Meta
	notVerified := ""

	if !metadataMatches(metadata, "institution", resp.Header, "X-Amz-Meta-Institution") {
		notVerified += "institution, "
	}
	if !metadataMatches(metadata, "bag", resp.Header, "X-Amz-Meta-Bag") {
		notVerified += "bag, "
	}
	if !metadataMatches(metadata, "bagpath", resp.Header, "X-Amz-Meta-Bagpath") {
		notVerified += "bagpath, "
	}
	if !metadataMatches(metadata, "md5", resp.Header, "X-Amz-Meta-Md5") {
		notVerified += "md5"
	}
	if len(notVerified) > 0 {
		return "", fmt.Errorf("Multi-part upload succeeded, but S3 does not return "+
			"the following metadata: %s", notVerified)
	}

	url = fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, fileName)
	return url, nil
}

// Returns true/false indicating whether a bucket exists.
func (client *S3Client) Exists(bucketName, key string) (bool, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.Exists(key)
}

// Returns a reader that lets you read data from bucket/key.
func (client *S3Client) GetReader(bucketName, key string) (io.ReadCloser, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.GetReader(key)
}

// Performs a HEAD request on an S3 object and returns the response.
// Check the response status code. You may get a 401 or 403 for files
// that don't exist, and the body will be an XML error message.
func (client *S3Client) Head(bucketName, key string) (*http.Response, error) {
	bucket := client.S3.Bucket(bucketName)
	return bucket.Head(key, nil)
}

func metadataMatches(metadata map[string][]string, key string, s3headers map[string][]string, headerName string) bool {
	metaValue, keyExists := metadata[key]
	headerValue, headerExists := s3headers[headerName]

	// If we didn't send this metadata in the first place, we
	// don't care if S3 has it.
	if !keyExists {
		return true
	}

	// If we sent the metadata, test whether S3 returned
	// what we sent.
	if keyExists && len(metaValue) > 0 && headerExists && len(headerValue) > 0 {
		return metaValue[0] == headerValue[0]
	}

	// If we get here, the key exists in the metadata we
	// sent, but not in the S3 headers.
	return false
}

*/
