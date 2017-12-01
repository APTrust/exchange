package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	opts := getUserOptions()
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(common.EXIT_USER_ERR)
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
	result := common.NewDownloadResult(opts, client)
	output := result.ToText()
	if opts.OutputFormat == "json" {
		var err error
		output, err = result.ToJson()
		if err != nil {
			fmt.Fprintf(os.Stderr, result.ToText())
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(common.EXIT_RUNTIME_ERR)
		}
	}
	fmt.Println(output)
	exitCode := common.EXIT_OK
	if strings.Contains(result.ErrorMessage, "NoSuchKey") {
		exitCode = common.EXIT_ITEM_NOT_FOUND
	} else if result.ErrorMessage != "" {
		exitCode = common.EXIT_RUNTIME_ERR
	}
	os.Exit(exitCode)
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	// If no bucket specified, the call below will
	// set the bucket to the restore bucket specified
	// in the config file, if possible.
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
	var version bool

	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&region, "region", constants.AWSVirginia, "AWS region to download from (default 'us-east-1')")
	flag.StringVar(&bucket, "bucket", "", "The bucket to fetch from (default is your restore bucket)")
	flag.StringVar(&key, "key", "", "The key you want to fetch")
	flag.StringVar(&dir, "dir", "", "Download file to this directory (default is current dir)")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
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

apt_download --key=<name/key of object to download> \
             [--bucket=<bucket to download from>] \
             [--config=<path to config file>] \
             [--region=<aws region to connect to>] \
			 [--dir=<download the object to this dir>] \
			 [--format=<'text' or 'json'>]

apt_download --help

apt_download --version

Options:

Note that option flags may be preceded by either one or two dashes,
so -option is the same as --option.

This program will get your AWS credentials from the config file, if
it can find one. Otherwise, it will get your AWS credentials from
the environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY".
If it can't find your AWS credentials, the download will fail.

--bucket is the name of the S3 bucket to download from. If omitted, the program
  will use the name of the restoration bucket in your partner config
  file. If you have no config file, or the restoration bucket isn't
  specified there, then you must specify the bucket on the command line.

--config is the optional path to your APTrust partner config file.
  If you omit this, the downloader uses the config at
  ~/.aptrust_partner.conf (Mac/Linux) or %HOMEPATH%\.aptrust_partner.conf
  (Windows) if that file exists. The config file should contain
  your AWS keys, and the locations of your receiving bucket, restore
  bucket, and the local directory into which you want items downloaded.
  For info about what should be in your config file, see
  https://wiki.aptrust.org/Partner_Tools

--dir is the directory into which you want to download the S3 file.
  If this is not specified, and you have an APTrust partner config
  file, apt_download will use the DownloadDir setting there. If
  there's no config file, your S3 item will be downloaded into the
  current working directory from which you're running this app.

--format is the format of the output printed to STDOUT when the download
  is complete. Options are 'text' and 'json', and the default is 'text'.

--help prints this help message and exits.

--key is the name of the item you want to download from S3. This param
  is required.

--region is the S3 region to connect to. This defaults to us-east-1. You
  generally should not have to set this for APTrust downloads,
  but you may set it on the command line to download non-APTrust
  files from your own buckets.

--version prints version info and exits.

Examples:

1. Download item "my_bag.tar" from your restoration bucket, using a
   custom APTrust partner config file.

   apt_download -bucket="aptrust.restore.test.edu" -key="my_bag.tar" -config="config.txt"

2. Download item "my_bag.tar" from a specified bucket and save it in
   /home/joy/downloads. Since there's no --config option here, this assumes
   that the environment vars "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY"
   are set.

   apt_download -key="my_bag.tar" -bucket="my.custom.bucket" -dir="/home/joy/downloads"

Exit codes:

0 - Bag was successfully downloaded.
1 - Operation could not be completed due to runtime, network, or server error
3 - Operation could not be completed due to usage error (e.g. missing params)
4 - Bag was not found in S3.
100 - Printed help or version message. No other operations attempted.
`
	fmt.Println(message)
}
