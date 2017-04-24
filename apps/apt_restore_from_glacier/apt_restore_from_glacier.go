package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/network"
	"os"
	"time"
)

type Options struct {
	Region string
	Bucket string
	Key    string
	Days   int64
}

type Output struct {
	// RequestTime is the timestamp the request was issued to AWS.
	RequestTime time.Time
	// RequestAccepted incidates whether or not AWS accepted the
	// retore request.
	RequestAccepted bool
	// ErrorMessage is a description of the error that occurred, if any.
	ErrorMessage string
	// RestoreStatus can be "Initiated", "AlreadyInProgress" or "Completed"
	RestoreStatus string
	// StorageClass is the item's current storage class (GLACIER or S3)
	StorageClass string
	// Bucket is the name of the bucket to which the item will be restored.
	// This is the same as the -bucket command-line param
	Bucket string
	// Key is the name of the item being restored. This is the same as
	// the -key command-line param.
	Key string
	// Days is the number of days the item will be kept in S3.
	Days int64
}

func main() {
	opts := parseCommandLine()
	aptRestore := network.NewS3Restore(
		opts.Region, opts.Bucket, opts.Key,
		"Standard", opts.Days)
	now := time.Now().UTC()
	aptRestore.Restore()
	restoreStatus := "Initiated"
	if aptRestore.RestoreAlreadyInProgress {
		restoreStatus = "AlreadyInProgress"
	}
	output := Output{
		Bucket:          opts.Bucket,
		Key:             opts.Key,
		Days:            opts.Days,
		RequestAccepted: (aptRestore.ErrorMessage == ""),
		ErrorMessage:    aptRestore.ErrorMessage,
		RestoreStatus:   restoreStatus,
		RequestTime:     now,
	}

	headClient := network.NewS3Head(opts.Region, opts.Bucket)
	headClient.Head(opts.Key)
	if headClient.ErrorMessage != "" {
		fmt.Fprintln(os.Stderr, "Error in HEAD request for", opts.Key, ":", headClient.ErrorMessage)
	}
	output.StorageClass = *headClient.Response.StorageClass

	jsonBytes, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintln(os.Stderr, "JSON marshal error:", err.Error())
		os.Exit(1)
	}
	fmt.Println(string(jsonBytes))
}

// See if you can figure out from the function name what this does.
func parseCommandLine() Options {
	var region string
	var bucket string
	var key string
	var days int
	flag.StringVar(&region, "region", "", "AWS region to check (required)")
	flag.StringVar(&bucket, "bucket", "", "The bucket to list (required)")
	flag.StringVar(&key, "key", "", "The key (object) to restore")
	flag.IntVar(&days, "days", 10, "How many days to keep the restored item in S3")

	flag.Parse()
	if region == "" || bucket == "" || key == "" {
		fmt.Fprintln(os.Stderr, "Params region, bucket, and key are required")
		printUsage()
		os.Exit(1)
	}
	options := Options{
		Region: region,
		Bucket: bucket,
		Key:    key,
		Days:   int64(days),
	}
	return options
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_restore_from_glacier: Restores an S3 item that has been archived into Glacier
back to the S3 bucket, so we can retrieve it. This program returns 0 on success
and non-zero on failure. See the exit codes below. It also prints details in JSON
format to STDOUT.

Usage: apt_restore_from_glacier -config=<path> \
             -region=<aws region> \
             -bucket=<bucket name> \
             -key=<key to restore> \
             -days=<number of days to keep restored item in S3>

Starred (*) params are required.

Param -config (*) is the path to the APTrust config file. It can be an
       absolute path, or config/<file.json> if it's in the config directory
       of $EXCHANGE_HOME.
Param -region (*) is the AWS region containing the S3/Glacier bucket.
       "us-east-1" is Virginia (DPN Glacier)
       "us-west-2" is Oregon (APTrust Glacier only)
Param -bucket (*) is the name of the bucket.
       "aptrust.dpn.preservation"     - DPN production bucket (us-east-1)
       "aptrust.preservation.oregon"  - APTrust Glacier bucket (us-west-2)
Param -key (*) is the key to restore from Glacier back into S3
Param -days is the number of days to leave the restored item in the S3 bucket.

Example
-------

Restore MyFile.txt from the Oregon Glacier bucket to the Oregon S3 bucket,
and leave the copy in Oregon S3 for ten days:

apt_restore_from_glacier -config=config/production.json -region="us-west-2" \
               -bucket="aptrust.preservation.storage" -key="MyFile.txt" -days=10


Return Codes
------------

0 - Request succeeded and program exited normally. You'll get this if the
    restore request succeeded, if it's already in progress, or if it
    has completed. Check the output for details.
1 - Request failed.


Output
------

Output for a newly initiated request will look like this (without the line
breaks):

{
 "RequestTime":"2017-04-24T18:15:07.547004384Z",
 "RequestAccepted":true,
 "ErrorMessage":"",
 "RestoreStatus":"Initiated",
 "StorageClass":"GLACIER",
 "Bucket":"aptrust.preservation.oregon",
 "Key":"000091f4-28ab-4ee1-b06e-b8bc771ceb40",
 "Days":2
}

Output for a request in progress will look like this (without the line
breaks):

{
 "RequestTime":"2017-04-24T18:17:02.454944Z",
 "RequestAccepted":true,
 "ErrorMessage":"",
 "RestoreStatus":"AlreadyInProgress",
 "StorageClass":"GLACIER",
 "Bucket":"aptrust.preservation.oregon",
 "Key":"000091f4-28ab-4ee1-b06e-b8bc771ceb40",
 "Days":2
}

`
	fmt.Println(message)
}
