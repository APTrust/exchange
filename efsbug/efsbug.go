package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// Change these as needed.
const (
	efsPath     = "/Users/apd4n/tmp" // "/mnt/efs/tmp"
	s3Region    = "us-east-1"
	s3Bucket    = "aptrust.test.preservation"
	s3Key       = "efs_bug_test_file.txt"
	contentType = "text/plain"
)

// Change this as needed. If we try to read our
// copied file (filePath2) before sending it to
// S3, the data makes it to S3 just fine. If we
// don't do an intermediate read, the S3 uploader
// creates a zero-byte file.
const doIntermediateRead = false

func main() {
	filePath1 := filepath.Join(efsPath, "file1.txt")
	filePath2 := filepath.Join(efsPath, "file2.txt")
	filePath3 := filepath.Join(efsPath, "file3.txt")

	// Write ~2GB of data to a file on the EFS volume
	createFile(filePath1)

	// Stat the file and print the first few bytes to STDOUT
	statAndPrint(filePath1)

	// Now copy that file, and try to read the copy.
	copyFile(filePath2, filePath1)

	// If we read the file once before sending it to S3,
	// it works. If we don't read the file once, it gets
	// sent to S3 as a zero-byte file
	if doIntermediateRead {
		statAndPrint(filePath2)
	}

	// Send the copy to S3
	f2, err := os.Open(filePath2)
	if err != nil {
		die(err.Error())
	}
	sendToS3(f2)
	f2.Close()

	// Retrieve the copy from S3
	fetchFromS3(filePath3)

	// See what's in that file we just pulled down from S3
	statAndPrint(filePath3)

	// Cleanup
	os.Remove(filePath1)
	os.Remove(filePath2)
	os.Remove(filePath3)

	fmt.Println("Don't forget to remove", s3Key, "from s3 bucket", s3Bucket)
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomBytes(size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	return buf
}

func createFile(pathToFile string) {
	// Get about 100k of data
	data := randomBytes(100000)
	file1, err := os.Create(pathToFile)
	if err != nil {
		die(err.Error())
	}
	defer file1.Close()

	// Write out about 200MB to disk
	for i := 0; i < 2000; i++ {
		n, err := file1.Write(data)
		if err != nil {
			die(err.Error())
		} else if n != len(data) {
			fmt.Fprintln(os.Stderr, "Create wrote only %d of %d bytes", n, len(data))
			os.Exit(1)
		}
	}
}

func copyFile(destPath, sourcePath string) {
	src, err := os.Open(sourcePath)
	if err != nil {
		die(err.Error())
	}
	defer src.Close()

	stat, err := src.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant stat file: %s", err.Error())
		os.Exit(1)
	}
	srcSize := stat.Size()

	dest, err := os.Create(destPath)
	if err != nil {
		die(err.Error())
	}
	defer dest.Close()

	n, err := io.Copy(dest, src)
	if n != srcSize {
		fmt.Fprintln(os.Stderr, "Copy wrote only %d of %d bytes", n, srcSize)
		os.Exit(1)
	}

	// Rewind this file to the beginning.
	// dest.Seek(0, io.SeekStart)
	// return dest
}

func statAndPrint(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		die(err.Error())
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant stat file: %s", err.Error())
	} else {
		fmt.Println("File size of", filePath, stat.Size())
	}
	buf := make([]byte, 256)
	n, err := f.Read(buf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant read file: %s", err.Error())
	} else {
		fmt.Println("Bytes read from", filePath, n)
		fmt.Println("Bytes:", string(buf))
	}
	fmt.Println("")
}

// This func takes a reader, just like the func in our production
// system. Our production code passes a forward-only tar reader
// in some cases, which is not capable of seeking. In other cases,
// it passes a file. The AWS uploader specifically checks for a
// reader that can seek, and takes advantage of it when possible.
// See the ReaderAtSeeker type in the AWS code here:
// https://github.com/aws/aws-sdk-go/blob/v1.5.8/service/s3/s3manager/upload.go#L411
func sendToS3(reader io.Reader) {
	uploadInput := &s3manager.UploadInput{
		Bucket:      aws.String(s3Bucket),
		Key:         aws.String(s3Key),
		ContentType: aws.String(contentType),
	}
	session, err := getS3Session(s3Region)
	if err != nil {
		die(err.Error())
	}
	uploader := s3manager.NewUploader(session)
	uploadInput.Body = reader
	started := time.Now().UTC()
	_, err = uploader.Upload(uploadInput)
	if err != nil {
		die(err.Error())
	}
	finished := time.Now().UTC()
	fmt.Println("Upload started at", started)
	fmt.Println("      finished at", finished)
}

func fetchFromS3(filePath string) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
	}
	session, err := getS3Session(s3Region)
	if err != nil {
		die(err.Error())
	}
	service := s3.New(session)
	if service == nil {
		die("No s3 download service")
	}
	resp, err := service.GetObject(params)
	if err != nil {
		die(err.Error())
	}
	defer resp.Body.Close()

	// Open a new file to store the data
	f3, err := os.Create(filePath)
	if err != nil {
		die(err.Error())
	}
	defer f3.Close()

	_, err = io.Copy(f3, resp.Body)
	if err != nil {
		die(err.Error())
	}
}

func getS3Session(awsRegion string) (*session.Session, error) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		die("AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY not set in environment")
	}
	creds := credentials.NewEnvCredentials()
	_session := session.New(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: creds,
	})
	if _session == nil {
		die("AWS Session returned nil")
	}
	return _session, nil
}

func die(errMsg string) {
	fmt.Fprintln(os.Stderr, errMsg)
	os.Exit(1)
}
