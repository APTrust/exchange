package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"strings"
	"time"
)

func main() {
	opts := getUserOptions()
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(common.EXIT_USER_ERR)
	}
	s3ObjList := network.NewS3ObjectList(
		opts.AccessKeyId,
		opts.SecretAccessKey,
		opts.Region,
		opts.Bucket,
		int64(opts.Limit))
	keepFetching := true
	headerPrinted := false
	keysFetched := 0
	for keepFetching {
		s3ObjList.GetList(opts.Prefix)
		if s3ObjList.ErrorMessage != "" {
			printError(s3ObjList.ErrorMessage)
			os.Exit(common.EXIT_RUNTIME_ERR)
		}
		if !headerPrinted {
			printHeader(opts)
			headerPrinted = true
		}
		printResult(s3ObjList.Response.Contents, opts.OutputFormat)
		keysFetched += len(s3ObjList.Response.Contents)
		keepFetching = *s3ObjList.Response.IsTruncated && keysFetched < opts.Limit
	}
	if opts.OutputFormat == "json" {
		fmt.Println("]")
	}
	os.Exit(common.EXIT_OK)
}

func printError(errMsg string) {
	fmt.Fprintln(os.Stderr, errMsg)
	if strings.Contains(errMsg, "AccessDenied") {
		fmt.Fprintln(os.Stderr, "Be sure the bucket name is correct. "+
			"S3 may return 'Access Denied' for buckets that don't exist.")
	}
}

func printHeader(opts *common.Options) {
	if opts.OutputFormat == "json" {
		fmt.Print("[")
	} else {
		fmt.Printf("%-20s  %-39s  %20s  %s\n", "Modified (UTC)", "ETag", "Size", "File")
	}
}

// printResult prints a batch of fiels from the list operation to STDOUT.
func printResult(items []*s3.Object, format string) {
	if format == "json" {
		for _, item := range items {
			jsonData, err := json.Marshal(item)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(common.EXIT_RUNTIME_ERR)
			}
			fmt.Print(string(jsonData))
		}
	} else {
		for _, item := range items {
			timestamp := item.LastModified.Format(time.RFC3339)[0:20]
			fmt.Printf("%-20s  %-39s  %20d  %s\n", timestamp, *item.ETag, *item.Size, *item.Key)
		}
	}
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	opts.SetAndVerifyListOptions()
	return opts
}

func parseCommandLine() *common.Options {
	var pathToConfigFile string
	var region string
	var bucket string
	var prefix string
	var outputFormat string
	var limit int
	var help bool
	var version bool

	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&region, "region", constants.AWSVirginia, "AWS region (default 'us-east-1')")
	flag.StringVar(&bucket, "bucket", "", "The bucket to list")
	flag.StringVar(&prefix, "prefix", "", "List objects whose name starts with this")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
	flag.IntVar(&limit, "limit", 100, "Max number of items to list (default 100)")
	flag.BoolVar(&help, "help", false, "Show help")
	flag.BoolVar(&version, "version", false, "Show version")

	flag.Parse()

	if version {
		fmt.Println(common.GetVersion())
		os.Exit(common.EXIT_NO_OP)
	}
	if help {
		printUsage()
		os.Exit(common.EXIT_NO_OP)
	}

	if bucket == "" {
		fmt.Fprintln(os.Stderr, "Please specify a bucket to list.")
		os.Exit(common.EXIT_USER_ERR)
	}

	opts := &common.Options{
		PathToConfigFile: pathToConfigFile,
		Region:           region,
		Bucket:           bucket,
		Prefix:           prefix,
		Limit:            limit,
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
apt_list lists files in an S3 bucket

Usage:

apt_list --bucket=<bucket to list> \
         [--config=<path to config file>] \
         [--format=<text|json>] \
         [--limit=100]
         [--prefix=<list items starting with prefix>] \
         [--region=<AWS region>]

Options:

Note that option flags may be preceded by either one or two dashes,
so -option is the same as --option.

This program will get your AWS credentials from the config file, if it
can find one. Otherwise, it will get your AWS credentials from the
environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY".
If it can't find your AWS credentials, it will exit with an error message.

--bucket is the name of the S3 bucket whose contents you want to list.

--config is the optional path to your APTrust partner config file.
  If you omit this, the uploader uses the config at
  ~/.aptrust_partner.conf (Mac/Linux) or %HOMEPATH%\.aptrust_partner.conf
  (Windows) if that file exists. The config file should contain
  your AWS keys, and the locations of your receiving bucket.
  For info about what should be in your config file, see
  https://wiki.aptrust.org/Partner_Tools

--format is the format of the output printed to STDOUT when the upload
  is complete. Options are 'text' and 'json', and the default is 'text'.

--help prints this help message and exits.

--limit is the maximum number of items to list. This defaults to 100.

--prefix filters the list of results to include only those items
  beginning with this prefix. E.g., if --prefix="bag200", the list
  will include only files whose names begin with bag200.

--region is the S3 region to connect to. This defaults to us-east-1.

--version prints version info and exits

Examples:

1. List everything in the bucket my_bucket:

   apt_list --bucket=my_bucket

2. List items in my bucket whose name begins with "image":

   apt_list --bucket=my_bucket --prefix=image

3. List items in my bucket whose name begins with "image" using AWS
   credentials from the config file "apt_config.txt":

   apt_list --bucket=my_bucket --prefix=image --config="apt_config.txt"

Exit codes:

0 - Program completed successfully.
1 - Operation could not be completed due to runtime, network, or server error
3 - Operation could not be completed due to usage error (e.g. missing params)
4 - Bucket does not exist.
100 - Printed help or version message. No other operations attempted.
`
	fmt.Println(message)
}
