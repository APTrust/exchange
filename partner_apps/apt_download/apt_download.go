package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"github.com/APTrust/exchange/util"
	"os"
	"path/filepath"
	"time"
)

type Result struct {
	Region                 string             `json:"region"`
	Bucket                 string             `json:"bucket"`
	Key                    string             `json:"key"`
	SavedTo                string             `json:"saved_to"`
	Md5                    string             `json:"md5"`
	Sha256                 string             `json:"sha256"`
	BytesDownloaded        int64              `json:"bytes_downloaded"`
	S3ContentLength        int64              `json:"s3_content_length"`
	S3ETag                 string             `json:"s3_etag"`
	S3LastModified         time.Time          `json:"s3_last_modified"`
	S3Metadata             map[string]*string `json:"s3_metadata"`
	S3PartsCount           int64              `json:"s3_parts_count"`
	S3ServerSideEncryption string             `json:"s3_server_side_encryption"`
	S3StorageClass         string             `json:"s3_storage_class"`
	S3VersionId            string             `json:"s3_version_id"`
	ErrorMessage           string             `json:"error_message"`
	AccessKeyFrom          string             `json:"access_key_from"`
	SecretKeyFrom          string             `json:"secret_key_from"`
}

var accessKeyFrom = "Could not find your AWS Access Key Id"
var secretKeyFrom = "Could not find your AWS Secret Key"

func main() {
	opts := getUserOptions()
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(1)
	}
	client := network.NewS3Download(
		opts.AccessKeyId,
		opts.SecretAccessKey,
		opts.Region,
		opts.Bucket,
		opts.Key,
		filepath.Join(opts.Dir, opts.Key),
		true,
		true,
	)
	client.Fetch()
	printResult(opts, client)
}

// Print the result of the fetch operation to STDOUT.
func printResult(opts *common.Options, client *network.S3Download) {
	result := Result{
		Region:          opts.Region,
		Bucket:          opts.Bucket,
		Key:             opts.Key,
		SavedTo:         client.LocalPath,
		Md5:             client.Md5Digest,
		Sha256:          client.Sha256Digest,
		BytesDownloaded: client.BytesCopied,
		ErrorMessage:    client.ErrorMessage,
		AccessKeyFrom:   accessKeyFrom,
		SecretKeyFrom:   secretKeyFrom,
	}
	if client.Response != nil {
		var contentLength int64
		var lastModified time.Time
		var partsCount int64
		if client.Response.ContentLength != nil {
			contentLength = *client.Response.ContentLength
		}
		if client.Response.LastModified != nil {
			lastModified = *client.Response.LastModified
		}
		if client.Response.PartsCount != nil {
			partsCount = *client.Response.PartsCount
		}
		if client.Response != nil {
			result.S3ContentLength = contentLength
			result.S3ETag = util.PointerToString(client.Response.ETag)
			result.S3LastModified = lastModified
			result.S3Metadata = client.Response.Metadata
			result.S3PartsCount = partsCount
			result.S3ServerSideEncryption = util.PointerToString(client.Response.ServerSideEncryption)
			result.S3StorageClass = util.PointerToString(client.Response.StorageClass)
			result.S3VersionId = util.PointerToString(client.Response.VersionId)
		}
	}
	exitCode := 0
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting result to JSON: %v", err)
	} else {
		fmt.Println(string(jsonBytes))
	}
	if err != nil || client.ErrorMessage != "" {
		exitCode = 1
	}
	os.Exit(exitCode)
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	opts.SetAndVerifyDownloadOptions()
	return opts
}

func parseCommandLine() *common.Options {
	var pathToConfigFile string
	var region string
	var bucket string
	var key string
	var dir string
	var outputFormat string
	var help bool
	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&region, "region", constants.AWSVirginia, "AWS region to download from (default 'us-east-1')")
	flag.StringVar(&bucket, "bucket", "", "The bucket to fetch from (default is your restore bucket)")
	flag.StringVar(&key, "key", "", "The key you want to fetch")
	flag.StringVar(&dir, "dir", "", "Download file to this directory (default is current dir)")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
	flag.BoolVar(&help, "help", false, "Show help")

	flag.Parse()

	opts := &common.Options{
		PathToConfigFile: pathToConfigFile,
		Region:           region,
		Bucket:           bucket,
		Key:              key,
		Dir:              dir,
		OutputFormat:     outputFormat,
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		opts.AccessKeyId = os.Getenv("AWS_ACCESS_KEY_ID")
		opts.AccessKeyFrom = "environment"
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		opts.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		opts.SecretKeyFrom = "environment"
	}

	return opts
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_download downloads a file from S3 to a local directory

Usage:

apt_download -config=<path to config file> \
			 -region=<aws region to connect to> \
			 -bucket=<bucket to download from> \
			 -key=<name/key of object to downlad> \
			 -dir=<download the object to this dir> \

Params:

Note that key is the only required param. This program will get your
AWS credentials from the config file, if it can find one. Otherwise,
it will get your AWS credentials from the environment variables
"AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY". If it can't find your
AWS credentials, the download will fail.

-config is the optional path to your APTrust partner config file.
		If you omit this, the downloader uses the config at
		~/.aptrust_partner.conf (Mac/Linux) or %HOMEPATH%\.aptrust_partner.conf
		(Windows) if that file exists. The config file should contain
		your AWS keys, and the locations of your receiving bucket, restore
		bucket, and the local directory into which you want items downloaded.
		For info about what should be in your config file, see
		https://sites.google.com/a/aptrust.org/member-wiki/partner-tools

-region is the S3 region to connect to. This defaults to us-east-1. You
		generally should not have to set this for APTrust downloads,
		but you may set it on the command line to download non-APTrust
		files from your own buckets.

-bucket is the name of the S3 bucket to download from. If this is not
		specified on the command line, apt_download will use the
		restoration bucket specified in your APTrust partner config file.
		See the -config option for more info.

-key    is the name of the item you want to download from S3. This param
		is required.

-dir    is the directory into which you want to download the S3 file.
		If this is not specified, and you have an APTrust partner config
		file, apt_download will use the DownloadDir setting there. If
		there's no config file, your S3 item will be downloaded into the
		current working directory from which you're running this app.

Examples:

1. Download item "my_bag.tar" from your restoration bucket, using your
   default APTrust partner config file in ~/.aptrust_partner.conf (Mac/Linux)
   or %HOMEPATH%\.aptrust_partner.conf

   apt_download -key="my_bag.tar"

2. Download item "my_bag.tar" from your restoration bucket, using a
   custom APTrust partner config file

   apt_download -key="my_bag.tar" -config="/home/joy/aptrust_config.txt"

3. Download item "my_bag.tar" from a specified bucket and save it in
   /home/joy/downloads

   apt_download -key="my_bag.tar" -bucket="my.custom.bucket" -dir="/home/joy/downloads"
`
	fmt.Println(message)
}
