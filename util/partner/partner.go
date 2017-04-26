package partner

import (
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"os/user"
	"path/filepath"
)

var Version string = "2.1"

var ConfigHelp string = `
Your config file should include the following name-value pairs,
separated by an equal sign. The file may also include comment lines,
which begin with a hash mark. Here's an example config file:

# Config for apt_upload and apt_download
AwsAccessKeyId = 123456789XYZ
AwsSecretAccessKey = THIS KEY INCLUDES SPACES AND DOES NOT NEED QUOTES
ReceivingBucket = 'aptrust.receive.test.edu'
RestorationBucket = "aptrust.restore.test.edu"
DownloadDir = "/home/josie/downloads"

If you prefer not to put your AWS keys in the config file, you can
put them into environment variables called AWS_ACCESS_KEY_ID
and AWS_SECRET_ACCESS_KEY.

ReceivingBucket is the name of the S3 bucket that will hold your
uploaded APTrust bags that are awaiting ingest.

RestorationBucket is the name of the S3 bucket that will hold your
restored APTrust bags.

DownloadDir is the name of the local directory in which to save
files downloaded from your APTrust restoration bucket. The APTrust
config currently does not expand ~ to your home directory, so use
an absolute path to be safe.
`

var BagSpecMessage = `
The full APTrust bagit specification is available at
https://sites.google.com/a/aptrust.org/member-wiki/basic-operations/bagging
`

// Prints the current version number to stdout.
func PrintVersion(appName string) {
	fmt.Printf("%s Version %s\n", appName, Version)
	fmt.Println("Academic Preservation Trust, 2017")
}

// Returns the name of the default APTrust partner config file.
func DefaultConfigFile() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	defaultConfigFile := filepath.Join(usr.HomeDir, ".aptrust_partner.conf")
	return defaultConfigFile, nil
}

// Returns true if the default config file exists.
func DefaultConfigFileExists() bool {
	filePath, err := DefaultConfigFile()
	return err == nil && fileutil.FileExists(filePath)
}
