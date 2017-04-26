package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/partner"
	"os"
	"path/filepath"
	"time"
)

type Options struct {
	PathToConfigFile string
	AccessKeyId      string
	SecretAccessKey  string
	Region           string
	Bucket           string
	Key              string
	Dir              string
	OutputFormat     string
}

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
}

func main() {
	opts := getUserOptions()
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
func printResult(opts *Options, client *network.S3Download) {
	result := Result{
		Region:          opts.Region,
		Bucket:          opts.Bucket,
		Key:             opts.Key,
		SavedTo:         client.LocalPath,
		Md5:             client.Md5Digest,
		Sha256:          client.Sha256Digest,
		BytesDownloaded: client.BytesCopied,
		ErrorMessage:    client.ErrorMessage,
	}
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
func getUserOptions() *Options {
	opts := parseCommandLine()
	verifyFormat(opts)
	ensureDirIsSet(opts)
	return opts
}

func parseCommandLine() *Options {
	var pathToConfigFile string
	var region string
	var bucket string
	var key string
	var dir string
	var outputFormat string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&region, "region", constants.AWSVirginia, "AWS region to download from (default 'us-east-1')")
	flag.StringVar(&bucket, "bucket", "", "The bucket to fetch from (default is your restore bucket)")
	flag.StringVar(&key, "key", "", "The key you want to fetch")
	flag.StringVar(&dir, "dir", "", "Download file to this directory (default is current dir)")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")

	flag.Parse()

	options := &Options{
		PathToConfigFile: pathToConfigFile,
		Region:           region,
		Bucket:           bucket,
		Key:              key,
		Dir:              dir,
		OutputFormat:     outputFormat,
	}
	return options
}

// Make sure the user specified a valid output format.
func verifyFormat(opts *Options) {
	if opts.OutputFormat != "text" && opts.OutputFormat != "json" {
		fmt.Fprintln(os.Stderr, "Param -format must be either 'text' or 'json'")
		os.Exit(1)
	}
}

// Make sure we have a directory to download the file into.
func ensureDirIsSet(opts *Options) {
	var err error
	dir := opts.Dir
	if dir == "" {
		dir, err = os.Getwd()
		if err != nil {
			dir, err = fileutil.RelativeToAbsPath(".")
			if err != nil {
				dir = "."
			}
		}
	}
	opts.Dir = dir
}

// If the user left some options unspecified on the command line,
// load them from the config file, if we can. If the user specified
// a config file, use that. Otherwise, use the default config file
// in ~/.aptrust_partner.conf or %HOMEPATH%\.aptrust_partner.conf
func mergeConfigFileOptions(opts *Options) {
	if opts.PathToConfigFile == "" && !partner.DefaultConfigFileExists() {
		return // there is no partner config to load
	}
	partnerConfig := loadConfigFile(opts)
	if opts.Bucket == "" {
		opts.Bucket = partnerConfig.RestorationBucket
	}
	if partnerConfig.AwsAccessKeyId != "" {
		opts.AccessKeyId = partnerConfig.AwsAccessKeyId
	} else {
		opts.AccessKeyId = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if partnerConfig.AwsSecretAccessKey != "" {
		opts.SecretAccessKey = partnerConfig.AwsSecretAccessKey
	} else {
		opts.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
}

// loadConfigFile loads the Partner Config file, which contains settings
// to connect to AWS S3. We must be able to load this file if certain
// command-line options are not specified.
func loadConfigFile(opts *Options) *models.PartnerConfig {
	var err error
	defaultConfigFile, _ := partner.DefaultConfigFile()
	if opts.PathToConfigFile == "" && partner.DefaultConfigFileExists() {
		opts.PathToConfigFile, err = fileutil.RelativeToAbsPath(defaultConfigFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot determine absolute path of %s: %v\n",
				opts.PathToConfigFile, err.Error())
			os.Exit(1)
		}
	}
	partnerConfig, err := models.LoadPartnerConfig(opts.PathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot load config file from %s: %v\n",
			opts.PathToConfigFile, err.Error())
		os.Exit(1)
	}
	warnings := partnerConfig.Warnings()
	for _, warning := range warnings {
		fmt.Fprintln(os.Stderr, warning)
	}
	if len(warnings) > 0 {
		os.Exit(1)
	}
	return partnerConfig
}

// Tell the user about the program.
func printUsage() {
	message := `
`
	fmt.Println(message)
}
