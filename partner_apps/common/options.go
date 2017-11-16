package common

import (
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/partner"
	"os"
	"strings"
)

type Options struct {
	// PathToConfigFile is the path the APTrust partner config
	// file. If not specified, this defaults to ~/.aptrust_partner.conf.
	// This can be omitted entirely if you supply the -bucket and -key
	// options on the command line. Any required options not specified
	// on the command line will be pulled from this file.
	PathToConfigFile string
	// AccessKeyId is your AWS access key id. Used for authentication.
	AccessKeyId string
	// AccessKeyFrom describes the source from which the Options object
	// loaded the AWS AccessKeyId. This is used only for testing and debugging.
	AccessKeyFrom string
	// APTrustAPIKey is the key to connect to APTrust REST API.
	// The key must belong to APTrustAPIUser.
	APTrustAPIKey string
	// APTrustAPIKeyFrom tells whether the API key came from the config
	// file or the environment.
	APTrustAPIKeyFrom string
	// APTrustAPIKey is the user email address to connect to APTrust REST API.
	APTrustAPIUser string
	// APTrustAPIUserFrom tells whether the API user came from the config
	// file or the environment.
	APTrustAPIUserFrom string
	// SecretAccessKey is the AWS Secret Access Key used to access your
	// S3 bucket.
	SecretAccessKey string
	// SecretKeyFrom describes the source from which the Options object
	// loaded the AWS SecretAccessKey. This is used only for testing and
	// debugging.
	SecretKeyFrom string
	// Region is the AWS S3 region to connect to.
	Region string
	// Bucket is the name of the bucket you're working with.
	Bucket string
	// Key is the name of the S3 key to download, list, or delete.
	Key string
	// Dir is the directory into which the S3 object should be downloaded.
	// This option is for downloads only.
	Dir string
	// ContentType is the content type of the object being uploaded
	// to S3. This option applies to uploads only, and can be left
	// empty.
	ContentType string
	// Metadata is optional metadata to be saved in S3 when uploading
	// a file.
	Metadata map[string]string
	// FileToUpload is the path the file that should be uploaded to S3.
	// This is required for apt_upload only, and is ignored elsewhere.
	FileToUpload string
	// PharosURL is the URL of the Pharos production or demo system.
	PharosURL string
	// OutputFormat specifies how the program should print its results
	// to STDOUT. Options are "text" and "json".
	OutputFormat string
	// Debug indicates whether we should print debug output to Stdout.
	Debug bool
	// error contains a list of errors describing why these options are
	// not valid for an operation like upload or download.
	errors []string
}

// SetAndVerifyDownloadOptions tries to fill in options
// that were not supplied on the command line with those
// specified in the APTrust partner config file. It also
// verifies that all required and allowed values are present.
// Check opts.HasErrors() after calling this, to see if we
// have sufficient options info to proceed with a download.
func (opts *Options) SetAndVerifyDownloadOptions() {
	opts.ClearErrors()
	if opts.OutputFormat == "" {
		opts.OutputFormat = "text"
	}
	opts.MergeConfigFileOptions()
	opts.VerifyOutputFormat()
	opts.EnsureDownloadDirIsSet()
	opts.VerifyRequiredDownloadOptions()
}

// SetAndVerifyUploadOptions
func (opts *Options) SetAndVerifyUploadOptions() {
	opts.ClearErrors()
	if opts.OutputFormat == "" {
		opts.OutputFormat = "text"
	}
	opts.MergeConfigFileOptions()
	opts.VerifyOutputFormat()
	opts.VerifyRequiredUploadOptions()
}

// VerifyRequiredDownloadOptions checks to see that all
// required download options are set.
func (opts *Options) VerifyRequiredDownloadOptions() {
	if opts.Key == "" {
		opts.addError("Param -key must be specified on the command line")
	}
	if opts.Bucket == "" {
		opts.addError("Param -bucket must be specified on the command line or in the config file")
	}
	if opts.AccessKeyId == "" {
		opts.addError("Cannot find AWS_ACCESS_KEY_ID in environment or config file")
	}
	if opts.SecretAccessKey == "" {
		opts.addError("Cannot find AWS_SECRET_ACCESS_KEY in environment or config file")
	}
}

// VerifyRequiredUploadOptions checks to see that all
// required upload options are set.
func (opts *Options) VerifyRequiredUploadOptions() {
	if opts.Bucket == "" {
		opts.addError("Param -bucket must be specified on the command line or in the config file")
	}
	if opts.AccessKeyId == "" {
		opts.addError("Cannot find AWS_ACCESS_KEY_ID in environment or config file")
	}
	if opts.SecretAccessKey == "" {
		opts.addError("Cannot find AWS_SECRET_ACCESS_KEY in environment or config file")
	}
	if opts.FileToUpload == "" {
		opts.addError("You must specify a file to upload")
	}
}

// VerifyOutputFormat makes sure the user specified a valid output format.
func (opts *Options) VerifyOutputFormat() {
	if opts.OutputFormat != "text" && opts.OutputFormat != "json" {
		opts.addError("Param -format must be either 'text' or 'json'")
	}
}

func (opts *Options) VerifyRequiredAPICredentials() {
	if opts.APTrustAPIUser == "" {
		opts.addError("Cannot find APTrust API user in environment or config file")
	}
	if opts.APTrustAPIKey == "" {
		opts.addError("Cannot find APTrust API key in environment or config file")
	}
}

// EnsureDownloadDirIsSet makes sure we have a directory to download the file into.
func (opts *Options) EnsureDownloadDirIsSet() {
	var err error
	// If the dir setting has a tilde, expand it to the user's
	// home directory. This call fails if the system cannot
	// determine the user.
	dir, _ := fileutil.ExpandTilde(opts.Dir)
	if dir == "" {
		dir = opts.Dir
	}
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

// MergeConfigFileOptions supplements command-line options with
// the default values the user specified in their APTrust
// parner config file.
//
// If the user left some options unspecified on the command line,
// load them from the config file, if we can. If the user specified
// a config file, use that. Otherwise, use the default config file
// in ~/.aptrust_partner.conf or %HOMEPATH%\.aptrust_partner.conf
func (opts *Options) MergeConfigFileOptions() {
	partnerConfig := &PartnerConfig{}
	if opts.PathToConfigFile != "" && partner.DefaultConfigFileExists() {
		var err error
		partnerConfig, err = opts.LoadConfigFile()
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}
	}
	if opts.Bucket == "" {
		opts.Bucket = partnerConfig.RestorationBucket
	}
	if opts.Dir == "" {
		opts.Dir = partnerConfig.DownloadDir
	}
	if opts.AccessKeyId == "" {
		if partnerConfig.AwsAccessKeyId != "" {
			opts.AccessKeyId = partnerConfig.AwsAccessKeyId
			opts.AccessKeyFrom = opts.PathToConfigFile
		} else {
			opts.AccessKeyId = os.Getenv("AWS_ACCESS_KEY_ID")
			opts.AccessKeyFrom = "ENV['AWS_ACCESS_KEY_ID']"
		}
	}
	if opts.SecretAccessKey == "" {
		if partnerConfig.AwsSecretAccessKey != "" {
			opts.SecretAccessKey = partnerConfig.AwsSecretAccessKey
			opts.AccessKeyFrom = opts.PathToConfigFile
		} else {
			opts.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
			opts.SecretKeyFrom = "ENV['AWS_SECRET_ACCESS_KEY']"
		}
	}
	if opts.APTrustAPIKey == "" {
		if partnerConfig.APTrustAPIKey != "" {
			opts.APTrustAPIKey = partnerConfig.APTrustAPIKey
			opts.APTrustAPIKeyFrom = opts.PathToConfigFile
		} else if os.Getenv("APTRUST_API_KEY") != "" {
			opts.APTrustAPIKey = os.Getenv("APTRUST_API_KEY")
			opts.APTrustAPIKeyFrom = "ENV['APTRUST_API_KEY']"
		} else if os.Getenv("PHAROS_API_KEY") != "" {
			opts.APTrustAPIKey = os.Getenv("PHAROS_API_KEY")
			opts.APTrustAPIKeyFrom = "ENV['PHAROS_API_KEY']"
		}
	}
	if opts.APTrustAPIUser == "" {
		if partnerConfig.APTrustAPIUser != "" {
			opts.APTrustAPIUser = partnerConfig.APTrustAPIUser
			opts.APTrustAPIUserFrom = opts.PathToConfigFile
		} else if os.Getenv("APTRUST_API_USER") != "" {
			opts.APTrustAPIUser = os.Getenv("APTRUST_API_USER")
			opts.APTrustAPIUserFrom = "ENV['APTRUST_API_USER']"
		} else if os.Getenv("PHAROS_API_USER") != "" {
			opts.APTrustAPIUser = os.Getenv("PHAROS_API_USER")
			opts.APTrustAPIUserFrom = "ENV['PHAROS_API_USER']"
		}
	}
}

// LoadConfigFile loads the Partner Config file, which contains settings
// to connect to AWS S3. We must be able to load this file if certain
// command-line options are not specified.
func (opts *Options) LoadConfigFile() (*PartnerConfig, error) {
	var err error
	defaultConfigFile, _ := partner.DefaultConfigFile()
	if opts.PathToConfigFile == "" && partner.DefaultConfigFileExists() {
		opts.PathToConfigFile, err = fileutil.RelativeToAbsPath(defaultConfigFile)
		if err != nil {
			opts.addError(fmt.Sprintf("Cannot determine absolute path of %s: %v\n",
				opts.PathToConfigFile, err.Error()))
			return nil, err
		}
	}
	partnerConfig, err := LoadPartnerConfig(opts.PathToConfigFile)
	if err != nil {
		opts.addError(fmt.Sprintf("Cannot load config file from %s: %v\n",
			opts.PathToConfigFile, err.Error()))
		return nil, err
	}
	//for _, warning := range partnerConfig.Warnings() {
	//	fmt.Fprintln(os.Stderr, "WARNING -", warning)
	//}
	return partnerConfig, nil
}

// addError adds an error to Options.Errors
func (opts *Options) addError(message string) {
	if opts.errors == nil {
		opts.errors = make([]string, 0)
	}
	opts.errors = append(opts.errors, message)
}

// Returns true of the options have any errors or missing
// required values.
func (opts *Options) HasErrors() bool {
	return opts.errors != nil && len(opts.errors) > 0
}

// AllErrorsAsString returns all errors as a single string,
// with each error ending in a newline. This is suitable
// for printing to STDOUT/STDERR.
func (opts *Options) AllErrorsAsString() string {
	errors := opts.Errors()
	if len(errors) > 0 {
		return strings.Join(errors, "\n")
	}
	return ""
}

// Errors returns a list of errors, such as invalid or
// missing params.
func (opts *Options) Errors() []string {
	if opts.errors == nil {
		opts.ClearErrors()
	}
	return opts.errors
}

// ClearErrors clears all errors. This is used in testing.
func (opts *Options) ClearErrors() {
	opts.errors = make([]string, 0)
}
