package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	EXIT_OK          = 0 // Item was successfully uploaded.
	EXIT_FAILED      = 1 // Upload failed.
	EXIT_NOT_EXISTS  = 2 // File does not exist.
	EXIT_USER_ERR    = 3 // Operation could not be completed due to usage error (e.g. missing params)
	EXIT_RUNTIME_ERR = 4 // Operation could not be completed due to runtime, network, or server error
	EXIT_HELP        = 5 // Printed help or version message. No other operations attempted.
)

func main() {
	opts := getUserOptions()
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(EXIT_USER_ERR)
	}
	uploadClient := network.NewS3Upload(
		opts.AccessKeyId,
		opts.SecretAccessKey,
		opts.Region,
		opts.Bucket,
		opts.Key,
		opts.ContentType)
	filestat, err := os.Stat(opts.FileToUpload)
	exitOnFileError(err)
	filesize := int64(0)
	if filestat != nil {
		filesize = filestat.Size()
	}
	file, err := os.Open(opts.FileToUpload)
	exitOnFileError(err)
	defer file.Close()
	if opts.Metadata != nil {
		for key, value := range opts.Metadata {
			uploadClient.AddMetadata(strings.ToLower(key), value)
		}
	}
	uploadClient.Send(file)
	exitCode := printResult(opts, uploadClient, filesize)
	os.Exit(exitCode)
}

func exitOnFileError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		if os.IsNotExist(err) {
			os.Exit(EXIT_NOT_EXISTS)
		}
		os.Exit(EXIT_RUNTIME_ERR)
	}
}

// printResults prints the results of the upload to STDOUT.
func printResult(opts *common.Options, uploadClient *network.S3Upload, filesize int64) int {
	headClient := network.NewS3Head(
		opts.AccessKeyId,
		opts.SecretAccessKey,
		opts.Region,
		opts.Bucket)
	headClient.Head(opts.Key)
	result := common.NewUploadResult(opts, uploadClient, headClient, filesize)
	output := result.ToText()
	if opts.OutputFormat == "json" {
		var err error
		output, err = result.ToJson()
		if err != nil {
			fmt.Fprintln(os.Stderr, result.ToText())
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(EXIT_RUNTIME_ERR)
		}
	}
	fmt.Println(output)
	if result.ErrorMessage != "" {
		return EXIT_RUNTIME_ERR
	}
	return EXIT_OK
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	opts.SetAndVerifyUploadOptions()
	return opts
}

func parseCommandLine() *common.Options {
	var pathToConfigFile string
	var region string
	var bucket string
	var key string
	var contentType string
	var outputFormat string
	var metadata string
	var help bool
	var version bool

	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&region, "region", constants.AWSVirginia, "AWS region to upload to (default 'us-east-1')")
	flag.StringVar(&bucket, "bucket", "", "The bucket to upload to (default is your receiving bucket)")
	flag.StringVar(&key, "key", "", "The name the object should have when stored in S3")
	flag.StringVar(&contentType, "contentType", "", "The mime type being uploaded (optional)")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
	flag.StringVar(&metadata, "metadata", "", "Optional metadata to store in S3")
	flag.BoolVar(&help, "help", false, "Show help")
	flag.BoolVar(&version, "version", false, "Show version")

	flag.Parse()

	if version {
		fmt.Println(common.GetVersion())
		os.Exit(EXIT_HELP)
	}
	if help {
		printUsage()
		os.Exit(EXIT_HELP)
	}

	if len(flag.Args()) < 1 {
		fmt.Fprintln(os.Stderr, "Please specify a file to upload.")
		os.Exit(EXIT_USER_ERR)
	}

	filePath, err := filepath.Abs(flag.Arg(0))
	exitOnFileError(err)
	if key == "" {
		key = path.Base(filePath)
	}

	opts := &common.Options{
		PathToConfigFile: pathToConfigFile,
		Region:           region,
		Bucket:           bucket,
		Key:              key,
		ContentType:      contentType,
		FileToUpload:     filePath,
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

	if metadata != "" {
		meta := make(map[string]string)
		err := json.Unmarshal([]byte(metadata), &meta)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Cannot parse metadata JSON:", err)
			os.Exit(EXIT_RUNTIME_ERR)
		}
		opts.Metadata = meta
	}

	return opts
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_upload uploads a file to S3.

Usage:

apt_upload [options] <file>

apt_upload -config=<path to config file> \
		   -region=<aws region to connect to> \
		   -bucket=<bucket to upload to> \
		   -key=<name/key of object to upload> \
		   -contentType=<mime type of upload> \
		   -format=<'text' or 'json'> \
		   -metadata=<json string> \
		   <file>

Params:

Note that file is the only required param. This program will get your
AWS credentials from the config file, if it can find one. Otherwise,
it will get your AWS credentials from the environment variables
"AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY". If it can't find your
AWS credentials, the upload will fail.

-config is the optional path to your APTrust partner config file.
		If you omit this, the uploader uses the config at
		~/.aptrust_partner.conf (Mac/Linux) or %HOMEPATH%\.aptrust_partner.conf
		(Windows) if that file exists. The config file should contain
		your AWS keys, and the locations of your receiving bucket.
		For info about what should be in your config file, see
		https://wiki.aptrust.org/Partner_Tools

-region is the S3 region to connect to. This defaults to us-east-1. You
		generally should not have to set this for APTrust uploads,
		but you may set it on the command line to upload non-APTrust
		files from your own buckets.

-bucket is the name of the S3 bucket to upload to. If this is not
		specified on the command line, apt_upload will use the
		restoration bucket specified in your APTrust partner config file.
		See the -config option for more info.

-key    if you want your uploaded file to have a different name in S3,
		specify that here. If you upload a file from /home/joy/my_file.txt,
		it will be put into your S3 bucket with the name "my_file.txt".
		Setting the -key option allows you to override that. So if
		-key='file_001.txt', /home/joy/my_file.txt will be saved to your
		S3 bucket with the name file_001.txt.

-contentType is the optional content type of the file you're uploading.
		If you choose to specify this, it should be in mime type format.
		For example, "image/jpeg" or "text/plain". You typically don't
		need to set this. If left unset, this usually defaults to something
		generic and unhelpful like "application/octet-stream".
		If you want to set it, you'll find a full list of mime types at
		https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types

-format is the format of the output printed to STDOUT when the upload
		is complete. Options are 'text' and 'json', and the default is
		'text'.

-metadata allows you to specify optional metadata, in json format, to be
		saved in S3 with your file. A metadata json string should look
		like this:

		-metadata='{"Bag":"my_bag","Bagpath":"data/Image001.tif","Institution":"virginia.edu","Md5":"12345","Sha256":"54321"}'

Examples:

1. Upload item "/home/joy/my_bag.tar" to your receiving bucket, using your
   default APTrust partner config file in ~/.aptrust_partner.conf (Mac/Linux)
   or %HOMEPATH%\.aptrust_partner.conf

   apt_upload /home/joy/my_bag.tar

2. Upload item "/home/joy/my_bag.tar" to your receiving bucket, using a
   custom APTrust partner config file

   apt_upload -config="/home/joy/aptrust_config.txt" /home/joy/my_bag.tar

3. Upload item "/home/joy/my_bag.tar" to a specified bucket with a custom
   name

   apt_upload -bucket="my.custom.bucket" -key="MySpecialFile.tar" /home/joy/my_bag.tar

Exit codes:

0 - Item was successfully uploaded.
1 - Upload failed.
2 - File does not exist.
3 - Operation could not be completed due to usage error (e.g. missing params)
4 - Operation could not be completed due to runtime, network, or server error
5 - Printed help or version message. No other operations attempted.
`
	fmt.Println(message)
}
