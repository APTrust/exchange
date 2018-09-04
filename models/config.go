package models

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
)

type WorkerConfig struct {
	// This describes how often the NSQ client should ping
	// the NSQ server to let it know it's still there. The
	// setting must be formatted like so:
	//
	// "800ms" for 800 milliseconds
	// "10s" for ten seconds
	// "1m" for one minute
	HeartbeatInterval string

	// The maximum number of times the worker should try to
	// process a job. If non-fatal errors cause a job to
	// fail, it will be requeued this number of times.
	// Fatal errors, such as invalid bags or attempts to
	// restore or delete non-existent files, will not be
	// retried.
	MaxAttempts uint16

	// Maximum number of jobs a worker will accept from the
	// queue at one time. Workers that may have to process
	// very long-running tasks, such as apt_prepare,
	// apt_store and apt_restore, should set this number
	// fairly low (20 or so) to prevent messages from
	// timing out.
	MaxInFlight int

	// If the NSQ server does not hear from a client that a
	// job is complete in this amount of time, the server
	// considers the job to have timed out and re-queues it.
	// Long-running jobs such as apt_prepare, apt_store,
	// apt_record and apt_restore will "touch" the NSQ message
	// as it moves through each channel in the processing pipeline.
	// The touch message tells NSQ that it's still working on
	// the job, and effectively resets NSQ's timer on that
	// message to zero. Still, very large bags in any of the
	// long-running processes will need a timeout of "180m" or
	// so to ensure completion.
	MessageTimeout string

	// Number of go routines used to perform network I/O,
	// such as fetching files from S3, storing files to S3,
	// and fetching/storing Fluctus data. If a worker does
	// no network I/O, this setting is ignored.
	NetworkConnections int

	// The name of the NSQ Channel the worker should read from.
	NsqChannel string

	// The name of the NSQ Topic the worker should listen to.
	NsqTopic string

	// This describes how long the NSQ client will wait for
	// a read from the NSQ server before timing out. The format
	// is the same as for HeartbeatInterval.
	ReadTimeout string

	// Number of go routines to start in the worker to
	// handle all work other than network I/O. Typically,
	// this should be close to the number of CPUs.
	Workers int

	// This describes how long the NSQ client will wait for
	// a write to the NSQ server to complete before timing out.
	// The format is the same as for HeartbeatInterval.
	WriteTimeout string
}

type Config struct {
	// ActiveConfig is the configuration currently
	// in use.
	ActiveConfig string

	// The name of the AWS region that hosts APTrust's Glacier files.
	APTrustGlacierRegion string

	// The name of the AWS region that hosts APTrust's S3 files.
	APTrustS3Region string

	// Config options specific to DPN services.
	DPN DPNConfig

	// Configuration options for apt_bag_delete
	BagDeleteWorker WorkerConfig

	// BagItVersion is the version number we write into the
	// bagit.txt file when we restore a bag.
	BagItVersion string

	// BagItEncoding is the encoding value we write into the
	// bagit.txt file when we restore a bag.
	BagItEncoding string

	// Location of the config file for bag validation.
	// Config will differ for APTrust and DPN. This is
	// for the APTrust config file.
	BagValidationConfigFile string

	// The bucket reader checks for new items in the receiving
	// buckets and queues them for ingest if they're not already
	// queued. During periods of heavy ingest, we may have
	// 10,000+ items in the receiving buckets. To avoid doing
	// 10,000+ REST calls to Pharos to check for existing WorkItems,
	// the bucket reader can do a handful of calls to cache
	// all new ingest records from the past X hours. We usually set
	// BucketReaderCacheHours to 24, to cache items that have appeared
	// in the past day. The bucket reader WILL look items that aren't
	// in the cache, but during peak hours when Pharos is under heavy
	// load, this will save the server a lot of work.
	BucketReaderCacheHours int

	// Set this in non-production environments to restore
	// intellectual objects to a custom bucket. If this is set,
	// all intellectual objects from all institutions will be
	// restored to this bucket.
	CustomRestoreBucket string

	// Should we delete the uploaded tar file from the receiving
	// bucket after successfully processing this bag?
	DeleteOnSuccess bool

	// Configuration options for apt_fetch
	FetchWorker WorkerConfig

	// Configuration options for apt_file_delete
	FileDeleteWorker WorkerConfig

	// Configuration options for apt_file_restore
	FileRestoreWorker WorkerConfig

	// Configuration options for apt_fixity, which
	// handles ongoing fixity checks.
	FixityWorker WorkerConfig

	// GlacierBucketVA is the name of the Glacier-only storage bucket in Virginia.
	GlacierBucketVA string

	// GlacierBucketOH is the name of the Glacier-only storage bucket in Ohio.
	GlacierBucketOH string

	// GlacierBucketOR is the name of the Glacier-only storage bucket in Oregon.
	// This bucket is distinct from the regular APTrust Glacier preservation
	// bucket in Oregon, since this one is for the Glacier-only storage class.
	GlacierBucketOR string

	// GlacierRegionVA is the name of the AWS region in which the Virginia
	// Glacier-only storage bucket is located.
	GlacierRegionVA string

	// GlacierRegionOH is the name of the AWS region in which the Ohio
	// Glacier-only storage bucket is located.
	GlacierRegionOH string

	// GlacierRegionOR is the name of the AWS region in which the Oregon
	// Glacier-only storage bucket is located.
	GlacierRegionOR string

	// Configuration options for apt_glacier_restore
	GlacierRestoreWorker WorkerConfig

	// LogDirectory is where we'll write our log files.
	LogDirectory string

	// LogLevel is defined in github.com/op/go-logging
	// and should be one of the following:
	// 1 - CRITICAL
	// 2 - ERROR
	// 3 - WARNING
	// 4 - NOTICE
	// 5 - INFO
	// 6 - DEBUG
	LogLevel logging.Level

	// If true, processes will log to STDERR in addition
	// to their standard log files. You really only want
	// to do this in development.
	LogToStderr bool

	// Maximum number of days allowed between scheduled
	// fixity checks. The fixity_reader periodically
	// queries Pharos for GenericFiles whose last
	// fixity check was greater than or equal to this
	// number of days ago. Those items are put into the
	// fixity_check queue.
	MaxDaysSinceFixityCheck int

	// MaxFileSize is the size in bytes of the largest
	// tar file we're willing to process. Set to zero
	// to process all files, regardless of size.
	// Set to some reasonably small size (100000 - 500000)
	// when you're running locally, or else you'll wind
	// up pulling down a huge amount of data from the
	// receiving buckets.
	MaxFileSize int64

	// NsqdHttpAddress tells us where to find the NSQ server
	// where we can read from and write to topics and channels.
	// It's typically something like "http://localhost:4151"
	NsqdHttpAddress string

	// NsqLookupd is the full HTTP(S) address of the NSQ Lookup
	// daemon, which is where our worker processes look first to
	// discover where they can find topics and channels. This is
	// typically something like "localhost:4161"
	NsqLookupd string

	// The version of the Pharos API we're using. This should
	// start with a v, like v1, v2.2, etc.
	PharosAPIVersion string

	// PharosURL is the URL of the Pharos server where
	// we will be recording results and metadata. This should
	// start with http:// or https://
	PharosURL string

	// The name of the preservation bucket to which we should
	// copy files for long-term storage.
	PreservationBucket string

	// ReceivingBuckets is a list of S3 receiving buckets to check
	// for incoming tar files.
	ReceivingBuckets []string

	// Configuration options for apt_record
	RecordWorker WorkerConfig

	// The bucket that stores a second copy of our perservation
	// files. This should be in a different region than the
	// preseration bucket. As of November 2014, the preservation
	// bucket is in Virginia, and the replication bucket is in
	// Oregon.
	ReplicationBucket string

	// The path to the local directory that will temporarily
	// hold files being copied from the preservartion bucket
	// in US East to the replication bucket in USWest2.
	ReplicationDirectory string

	// RestoreDirectory is the directory in which we will
	// rebuild IntellectualObject before sending them
	// off to the S3 restoration bucket.
	RestoreDirectory string

	// If true, we should restore bags to our partners' test
	// restoration buckets instead of the usual restoration
	// buckets. This should be true only in the demo config,
	// which is what we run on test.aptrust.org. Also note
	// that CustomRestoreBucket overrides this.
	RestoreToTestBuckets bool

	// Configuration options for apt_restore
	RestoreWorker WorkerConfig

	// SkipAlreadyProcessed indicates whether or not the
	// bucket_reader should  put successfully-processed items into
	// NSQ for re-processing. This is amost always set to false.
	// The exception is when we deliberately want to reprocess
	// items to test code changes.
	SkipAlreadyProcessed bool

	// Configuration options for apt_store
	StoreWorker WorkerConfig

	// TarDirectory is the directory in which we will
	// untar files from S3. This should be on a volume
	// with lots of free disk space.
	TarDirectory string

	// UseVolumeService describes whether to use volume_service or
	// to try to reserve disk space before downloading and processing
	// bags. You'll want to use this service on systems with a fixed
	// amount of disk space, so that APTrust and DPN services don't
	// try to download bags that won't fit in the remaining disk space.
	// When this is on, and the volume_service is running, APTrust and
	// DPN services will simply reque items that require more disk space
	// than is currently available. UseVolumeService should be false
	// (off) when using Amazon's EFS volumes because querying EFS volumes
	// for available space often returns an error, and that causes items
	// to be requeued when they should be processed, and EFS volumes
	// are virtually guaranteed to have more space than we need to process
	// bags.
	UseVolumeService bool

	// The port number, on localhost, where the HTTP
	// VolumeService should run. This is always on
	// 127.0.0.1, because it has to access the same
	// volumes and mounts as the locally running
	// services.
	VolumeServicePort int
}

// This returns the configuration that the user requested,
// which is specified in the -config flag when we run a
// program from the command line
func LoadConfigFile(pathToConfigFile string) (*Config, error) {
	file, err := fileutil.LoadRelativeFile(pathToConfigFile)
	if err != nil {
		detailedError := fmt.Errorf("Error reading config file '%s': %v\n",
			pathToConfigFile, err)
		return nil, detailedError
	}
	config := &Config{}
	err = json.Unmarshal(file, config)
	if err != nil {
		detailedError := fmt.Errorf("Error parsing JSON from config file '%s': %v",
			pathToConfigFile, err)
		return nil, detailedError
	}
	config.ActiveConfig = pathToConfigFile
	return config, nil
}

// Ensures that the logging directory exists, creating it if necessary.
// Returns the absolute path the logging directory.
//
// TODO: Rename this, since it's ensuring more than just the existence
// of the log directory.
func (config *Config) EnsureLogDirectory() (string, error) {
	config.ExpandFilePaths()
	err := config.createDirectories()
	if err != nil {
		return "", err
	}
	return config.AbsLogDirectory(), nil
}

func (config *Config) AbsLogDirectory() string {
	absLogDir, err := filepath.Abs(config.LogDirectory)
	if err != nil {
		msg := fmt.Sprintf("Cannot get absolute path to log directory. "+
			"config.LogDirectory is set to '%s'", config.LogDirectory)
		panic(msg)
	}
	return absLogDir
}

func (config *Config) EnsurePharosConfig() error {
	if config.PharosURL == "" {
		return fmt.Errorf("PharosUrl is missing from config file")
	}
	if os.Getenv("PHAROS_API_USER") == "" {
		return fmt.Errorf("Environment variable PHAROS_API_USER is not set")
	}
	if os.Getenv("PHAROS_API_KEY") == "" {
		return fmt.Errorf("Environment variable PHAROS_API_KEY is not set")
	}
	return nil
}

// Expands ~ file paths and bag validation config file relative
// paths to absolute paths.
func (config *Config) ExpandFilePaths() {
	expanded, err := fileutil.ExpandTilde(config.TarDirectory)
	if err == nil {
		config.TarDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.LogDirectory)
	if err == nil {
		config.LogDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.RestoreDirectory)
	if err == nil {
		config.RestoreDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.ReplicationDirectory)
	if err == nil {
		config.ReplicationDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.DPN.StagingDirectory)
	if err == nil {
		config.DPN.StagingDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.DPN.RemoteNodeHomeDirectory)
	if err == nil {
		config.DPN.RemoteNodeHomeDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.DPN.LogDirectory)
	if err == nil {
		config.DPN.LogDirectory = expanded
	}

	// Convert bag validation config files from relative to absolute paths.
	absPath, _ := filepath.Abs(config.BagValidationConfigFile)
	if absPath != config.BagValidationConfigFile {
		expanded, err = fileutil.RelativeToAbsPath(config.BagValidationConfigFile)
		if err == nil {
			config.BagValidationConfigFile = expanded
		}
	}
	absPath, _ = filepath.Abs(config.DPN.BagValidationConfigFile)
	if absPath != config.DPN.BagValidationConfigFile {
		expanded, err = fileutil.RelativeToAbsPath(config.DPN.BagValidationConfigFile)
		if err == nil {
			config.DPN.BagValidationConfigFile = expanded
		}
	}
}

func (config *Config) createDirectories() error {
	if config.TarDirectory == "" {
		return fmt.Errorf("You must define config.TarDirectory")
	}
	if config.LogDirectory == "" {
		return fmt.Errorf("You must define config.LogDirectory")
	}
	if config.RestoreDirectory == "" {
		return fmt.Errorf("You must define config.RestoreDirectory")
	}
	if config.ReplicationDirectory == "" {
		return fmt.Errorf("You must define config.ReplicationDirectory")
	}
	if !fileutil.FileExists(config.TarDirectory) {
		err := os.MkdirAll(config.TarDirectory, 0755)
		if err != nil {
			return err
		}
	}
	if !fileutil.FileExists(config.LogDirectory) {
		err := os.MkdirAll(config.LogDirectory, 0755)
		if err != nil {
			return err
		}
	}
	if !fileutil.FileExists(config.RestoreDirectory) {
		err := os.MkdirAll(config.RestoreDirectory, 0755)
		if err != nil {
			return err
		}
	}
	if !fileutil.FileExists(config.ReplicationDirectory) {
		err := os.MkdirAll(config.ReplicationDirectory, 0755)
		if err != nil {
			return err
		}
	}
	// TODO: Test these two
	if config.DPN.LogDirectory != "" && !fileutil.FileExists(config.DPN.LogDirectory) {
		err := os.MkdirAll(config.DPN.LogDirectory, 0755)
		if err != nil {
			return err
		}
	}
	if config.DPN.StagingDirectory != "" && !fileutil.FileExists(config.DPN.StagingDirectory) {
		err := os.MkdirAll(config.DPN.StagingDirectory, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func (config *Config) StorageRegionAndBucketFor(storageOption string) (region string, bucket string, err error) {
	if storageOption == constants.StorageStandard {
		region = config.APTrustS3Region
		bucket = config.PreservationBucket
	} else if storageOption == constants.StorageGlacierVA {
		region = config.GlacierRegionVA
		bucket = config.GlacierBucketVA
	} else if storageOption == constants.StorageGlacierOH {
		region = config.GlacierRegionOH
		bucket = config.GlacierBucketOH
	} else if storageOption == constants.StorageGlacierOR {
		region = config.GlacierRegionOR
		bucket = config.GlacierBucketOR
	} else {
		err = fmt.Errorf("Unknown Storage Option: %s", storageOption)
	}
	return region, bucket, err
}

// TestsAreRunning returns true if we're running unit or integration
// tests; false otherwise.
func (config *Config) TestsAreRunning() bool {
	return flag.Lookup("test.v") != nil
}

// GetAWSAccessKeyId returns the AWS Access Key ID from the environment,
// or an empty string if the ENV var isn't set. In test context, this
// returns a dummy key id so we don't get an error in the Travis CI
// environment.
func (config *Config) GetAWSAccessKeyId() string {
	keyId := os.Getenv("AWS_ACCESS_KEY_ID")
	if keyId == "" && config.TestsAreRunning() {
		keyId = "TestKeyId"
	}
	return keyId
}

// GetAWSAccessSecretAccessKey returns the AWS Secret Access Key
// from the environment, or an empty string if the ENV var isn't set.
// In test context, this returns a dummy key id so we don't get an
// error in the Travis CI environment.
func (config *Config) GetAWSSecretAccessKey() string {
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" && config.TestsAreRunning() {
		secretKey = "TestSecretKey"
	}
	return secretKey
}

// DefaultMetadata includes mostly static information about bags
// that APTrust packages for DPN.
type DefaultMetadata struct {
	Comment                string
	BagItVersion           string
	BagItEncoding          string
	IngestNodeName         string
	IngestNodeAddress      string
	IngestNodeContactName  string
	IngestNodeContactEmail string
}

// Config options for our DPN REST client.
type RestClientConfig struct {
	Comment         string
	LocalServiceURL string
	LocalAPIRoot    string
	LocalAuthToken  string
}

type DPNConfig struct {
	// Should we accept self-signed and otherwise invalid SSL
	// certificates? We need to do this in testing, but it
	// should not be allowed in production. Bools in Go default
	// to false, so if this is not set in config, we should be
	// safe.
	AcceptInvalidSSLCerts bool

	// Location of the config file for bag validation.
	// Config will differ for APTrust and DPN. This is
	// for the DPN config file.
	BagValidationConfigFile string

	// Default metadata that goes into bags produced at our node.
	DefaultMetadata DefaultMetadata

	// DPNAPIVersion is the current version of the DPN REST API.
	// This should be a string in the format api-v1, api-v2, etc.
	DPNAPIVersion string

	// The name of the AWS region that hosts DPN's Glacier files.
	DPNGlacierRegion string

	// DPNCopyWorker copies tarred bags from other nodes into our
	// DPN staging area, so we can replication them. Currently,
	// copying is done by rsync over ssh.
	DPNCopyWorker WorkerConfig

	// DPNFixityWorker processes requests to run fixity checks on
	// bags that have been copied from Glacier through S3 into
	// local storage.
	DPNFixityWorker WorkerConfig

	// DPNGlacierRestoreWorker processes requests to move files
	// from Glacier storage to S3 storage.
	DPNGlacierRestoreWorker WorkerConfig

	// DPNIngestStoreWorker copies DPN bags ingested from APTrust
	// to AWS Glacier.
	DPNIngestStoreWorker WorkerConfig

	// DPNPackageWorker packages APTrust IntellectualObjects into
	// DPN bags, so they can be ingested into DPN.
	DPNPackageWorker WorkerConfig

	// The name of the long-term storage bucket for DPN
	DPNPreservationBucket string

	// DPNIngestRecordWorker records DPN ingest events in Pharos
	// and in the DPN REST server.
	DPNIngestRecordWorker WorkerConfig

	// DPNReplicationStoreWorker copies DPN bags replicated from
	// other nodes to AWS Glacier.
	DPNReplicationStoreWorker WorkerConfig

	// DPNRestoreWorker processed RestoreTransfer requests.
	DPNRestoreWorker WorkerConfig

	// DPNS3DownloadWorker processes requests to move files
	// from S3 to local storage. These files have previously
	// been moved from Glacier to S3 by the DPNGlacierRestoreWorker.
	// We do the downlad from S3 to local before checking fixity,
	// which in DPN requires us to parse and validate the entire
	// tarred bag, then calculate the sha256 checksum of the bag's
	// tag manifest.
	DPNS3DownloadWorker WorkerConfig

	// DPNValidationWorker validates DPN bags that we are replicating
	// from other nodes.
	DPNValidationWorker WorkerConfig

	// LocalNode is the namespace of the node this code is running on.
	// E.g. "aptrust", "chron", "hathi", "tdr", "sdr"
	LocalNode string

	// Where should DPN service logs go?
	LogDirectory string

	// Log level (4 = debug)
	LogLevel logging.Level

	// Should we log to Stderr in addition to writing to
	// the log file?
	LogToStderr bool

	// RemoteNodeHomeDirectory is the prefix to the home directory
	// for remote DPN nodes that connect to our node via rsync/ssh.
	// On demo and production, this should be "/home". The full home
	// directory for a user like tdr  would be "/home/dpn.tdr".
	// On a local dev or test machine,this can be any path the user
	// has full read/write access to.
	RemoteNodeHomeDirectory string

	// Number of nodes we should replicate bags to.
	ReplicateToNumNodes int

	// Settings for connecting to our own REST service
	RestClient RestClientConfig

	// RemoteNodeAdminTokensForTesting are used in integration
	// tests only, when we want to perform admin-only operations,
	// such as creating bags and replication requests on a remote
	// node in the test cluster.
	RemoteNodeAdminTokensForTesting map[string]string

	// API Tokens for connecting to remote nodes
	RemoteNodeTokens map[string]string

	// URLs for remote nodes. Set these only if you want to
	// override the node URLs we get back from our local
	// DPN REST server.
	RemoteNodeURLs map[string]string

	// The local directory for DPN staging. We store DPN bags
	// here while they await transfer to the DPN preservation
	// bucket and while they await replication to other nodes.
	StagingDirectory string

	// When copying bags from remote nodes, should we use rsync
	// over SSH (true) or just plain rsync (false)? For local
	// integration testing, this should be false.
	UseSSHWithRsync bool
}
