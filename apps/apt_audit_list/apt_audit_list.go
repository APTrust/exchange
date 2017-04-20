package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

type Options struct {
	PathToConfigFile string
	Region           string
	Bucket           string
	KeyPrefix        string
	Format           string
	Limit            int
	Concurrency      int
}

func main() {
	opts := parseCommandLine()
	config, err := models.LoadConfigFile(opts.PathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)

	aptAuditList, err := workers.NewAPTAuditList(_context,
		opts.Region, opts.Bucket, opts.KeyPrefix,
		opts.Format, opts.Limit, opts.Concurrency)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	aptAuditList.Run()
}

// See if you can figure out from the function name what this does.
func parseCommandLine() Options {
	var pathToConfigFile string
	var region string
	var bucket string
	var keyPrefix string
	var format string
	var limit int
	var concurrency int
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file (required)")
	flag.StringVar(&region, "region", "", "AWS region to check (required)")
	flag.StringVar(&bucket, "bucket", "", "The bucket to list (required)")
	flag.StringVar(&keyPrefix, "prefix", "", "List keys starting with this prefix")
	flag.StringVar(&format, "format", "tsv", "Output data in this format")
	flag.IntVar(&limit, "limit", 50, "List no more than this many files")
	flag.IntVar(&concurrency, "concurrency", 4, "Use this many concurrent HTTP connections")

	flag.Parse()
	if pathToConfigFile == "" || region == "" || bucket == "" {
		fmt.Fprintln(os.Stderr, "Params config, region, and bucket are required")
		printUsage()
		os.Exit(1)
	}
	options := Options{
		PathToConfigFile: pathToConfigFile,
		Region:           region,
		Bucket:           bucket,
		KeyPrefix:        keyPrefix,
		Format:           format,
		Limit:            limit,
		Concurrency:      concurrency,
	}
	return options
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_bucket_reader: Reads the contents of S3 receiving buckets, and creates
WorkItem entries and NSQ entries for bags awaiting ingest in those buckets.

Usage: apt_audit_list -config=<path> \
                      -region=<aws region> \
                      -bucket=<bucket name> \
                      -prefix=<key prefix> \
                      -format=<output format> \
                      -limit=<max items to list> \
                      -concurrency=<max simultaneous clients>

Starred (*) params are required.

Param -config (*) is the path to the APTrust config file. It can be an
       absolute path, or config/<file.json> if it's in the config directory
       of $EXCHANGE_HOME.
Param -region (*) is the AWS region containing the S3 or Glacier bucket.
       "us-east-1" is Virginia (All buckets other than APTrust Glacier)
       "us-west-2" is Oregon (APTrust Glacier only)
Param -bucket (*) is the name of the bucket.
       "aptrust.dpn.preservation"     - DPN production bucket (us-east-1)
       "aptrust.dpn.test"             - DPN test bucket (us-east-1)
       "aptrust.preservation.oregon"  - APTrust Glacier bucket (us-west-2)
       "aptrust.preservation.storage" - APTrust production S3 bucket (us-east-1)
       "aptrust.test.preservation"    - APTrust test bucket (us-east-1)
Param -prefix will limit the list to keys beginning with the specified
       prefix. To list all keys, omit the prefix flag.
Param -format specifies the output format. The default is "tsv",
       but any of the following are valid:
       "json" - JSON data
       "csv"  - Comma-separated values
       "tsv"  - Tab-separated values
Param -limit is the maximum number of records to fetch. The default is 50.
       If you want to list everything in a bucket, set limit to something
       like 2000000000 (two billion)
Param -concurrency is the number of concurrent HTTP requests to issue when
       building the list. The default is 4, and the max is 32.

Examples
--------

List the first 100 items from the main preservation bucket in S3/Virginia:

apt_audit_list -config=config/production.json -region="us-east-1" \
               -bucket="aptrust.preservation.storage" -limit=100


List the first 100 items from Glacier/Oregon whose keys start with "a00"
and use JSON as the output format:

apt_audit_list -config=config/production.json -region="us-west-2" \
               -bucket="aptrust.preservation.oregon" -prefix="a00" \
               -limit=100 -format=json

List the first 100 items in the DPN preservation bucket, using 10
concurrent connections:

apt_audit_list -config=config/production.json -region="us-east-1" \
               -bucket="aptrust.dpn.preservaton" -limit=100 \
               -concurrency=10

`
	fmt.Println(message)
}
