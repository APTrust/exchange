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

Param -config (*) is the path to the APTrust config file.
Param -region (*) is the AWS region containing the S3 or Glacier bucket.
       "us-east-1" is Virginia (All buckets other than APTrust Glacier)
       "us-west-2" is Oregon (APTrust Glacier only)
Param -bucket (*) is the name of the bucket.
       "aptrust-dpn-preservation" is the DPN production bucket
       "aptrust-dpn-test" is the DPN test bucket
       "aptrust-preservation-oregon" is the APTrust Glacier bucket
       "aptrust-preservation-storage" is the APTrust production S3 bucket
       "aptrust-test-preservation" is the APTrust test bucket
Param -prefix will limit the list to keys beginning with the specified
       prefix. To list all keys, omit the prefix flag.
Param -format specifies the output format. The default is "tsv",
       but any of the following are valid:
       "json" - JSON data
       "csv"  - Comma-separated values
       "tsv"  - Tab-separated values
Param -limit is the maximum number of records to fetch. The default is 50.
Param -concurrency is the number of concurrent HTTP requests to issue when
       building the list. The default is 4, and the max is 32.
`
	fmt.Println(message)
}
