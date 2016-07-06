package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
	"strings"
)

type WorkerConfig struct {
	// This describes how often the NSQ client should ping
	// the NSQ server to let it know it's still there. The
	// setting must be formatted like so:
	//
	// "800ms" for 800 milliseconds
	// "10s" for ten seconds
	// "1m" for one minute
	HeartbeatInterval  string

	// The maximum number of times the worker should try to
	// process a job. If non-fatal errors cause a job to
	// fail, it will be requeued this number of times.
	// Fatal errors, such as invalid bags or attempts to
	// restore or delete non-existent files, will not be
	// retried.
	MaxAttempts        uint16

	// Maximum number of jobs a worker will accept from the
	// queue at one time. Workers that may have to process
	// very long-running tasks, such as apt_prepare,
	// apt_store and apt_restore, should set this number
	// fairly low (20 or so) to prevent messages from
	// timing out.
	MaxInFlight        int

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
	MessageTimeout     string

	// Number of go routines used to perform network I/O,
	// such as fetching files from S3, storing files to S3,
	// and fetching/storing Fluctus data. If a worker does
	// no network I/O (such as the TroubleWorker), this
	// setting is ignored.
	NetworkConnections int

	// The name of the NSQ Channel the worker should read from.
	NsqChannel         string

	// The name of the NSQ Topic the worker should listen to.
	NsqTopic           string

	// This describes how long the NSQ client will wait for
	// a read from the NSQ server before timing out. The format
	// is the same as for HeartbeatInterval.
	ReadTimeout        string

	// Number of go routines to start in the worker to
	// handle all work other than network I/O. Typically,
	// this should be close to the number of CPUs.
	Workers            int

	// This describes how long the NSQ client will wait for
	// a write to the NSQ server to complete before timing out.
	// The format is the same as for HeartbeatInterval.
	WriteTimeout       string
}


type Config struct {
	// ActiveConfig is the configuration currently
	// in use.
	ActiveConfig            string

	// Configuration options for apt_bag_delete
	BagDeleteWorker         WorkerConfig

	// Set this in non-production environments to restore
	// intellectual objects to a custom bucket. If this is set,
	// all intellectual objects from all institutions will be
	// restored to this bucket.
	CustomRestoreBucket     string

	// Should we delete the uploaded tar file from the receiving
	// bucket after successfully processing this bag?
	DeleteOnSuccess         bool

	// DPNCopyWorker copies tarred bags from other nodes into our
	// DPN staging area, so we can replication them. Currently,
	// copying is done by rsync over ssh.
	DPNCopyWorker           WorkerConfig

	// DPNHomeDirectory is the prefix to the home directory
	// for all DPN users. On demo and production, this should
	// be "/home". The full home directory for a user like tdr
	// would be "/home/dpn.tdr". On a local dev or test machine,
	// DPNHomeDirectory can be any path the user has full read/write
	// access to.
	DPNHomeDirectory        string

	// DPNPackageWorker records details about fixity checks
	// that could not be completed.
	DPNPackageWorker        WorkerConfig

	// The name of the long-term storage bucket for DPN
	DPNPreservationBucket   string

	// DPNRecordWorker records DPN storage events in Pharos
	// and through the DPN REST API.
	DPNRecordWorker         WorkerConfig

	// The local directory for DPN staging. We store DPN bags
	// here while they await transfer to the DPN preservation
	// bucket and while they await replication to other nodes.
	DPNStagingDirectory     string

	// DPNStoreWorker copies DPN bags to AWS Glacier.
	DPNStoreWorker          WorkerConfig

	// DPNTroubleWorker records failed DPN tasks in the DPN
	// trouble queue.
	DPNTroubleWorker        WorkerConfig

	// DPNValidationWorker validates DPN bags.
	DPNValidationWorker     WorkerConfig

	// FailedFixityWorker records details about fixity checks
	// that could not be completed.
	FailedFixityWorker      WorkerConfig

	// FailedReplicationWorker records details about failed
	// attempts to copy generic files to the S3 replication
	// bucket in Oregon.
	FailedReplicationWorker WorkerConfig

	// Configuration options for apt_file_delete
	FileDeleteWorker        WorkerConfig

	// Configuration options for apt_fixity, which
	// handles ongoing fixity checks.
	FixityWorker            WorkerConfig

	// The version of the Pharos API we're using. This should
	// start with a v, like v1, v2.2, etc.
	PharosAPIVersion       string

	// PharosURL is the URL of the Pharos server where
	// we will be recording results and metadata. This should
	// start with http:// or https://
	PharosURL              string

	// LogDirectory is where we'll write our log files.
	LogDirectory            string

	// LogLevel is defined in github.com/op/go-logging
	// and should be one of the following:
	// 1 - CRITICAL
	// 2 - ERROR
	// 3 - WARNING
	// 4 - NOTICE
	// 5 - INFO
	// 6 - DEBUG
	LogLevel                logging.Level

	// If true, processes will log to STDERR in addition
	// to their standard log files. You really only want
	// to do this in development.
	LogToStderr             bool

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
	MaxFileSize             int64

	// Configuration options for apt_prepare
	PrepareWorker           WorkerConfig

	// The name of the preservation bucket to which we should
	// copy files for long-term storage.
	PreservationBucket      string

	// ReceivingBuckets is a list of S3 receiving buckets to check
	// for incoming tar files.
	ReceivingBuckets        []string

	// Configuration options for apt_record
	RecordWorker            WorkerConfig

	// The bucket that stores a second copy of our perservation
	// files. This should be in a different region than the
	// preseration bucket. As of November 2014, the preservation
	// bucket is in Virginia, and the replication bucket is in
	// Oregon.
	ReplicationBucket       string

	// The path to the local directory that will temporarily
	// hold files being copied from the preservartion bucket
	// in US East to the replication bucket in USWest2.
	ReplicationDirectory    string

	// Configuration options for apt_replicate
	ReplicationWorker       WorkerConfig

	// RestoreDirectory is the directory in which we will
	// rebuild IntellectualObject before sending them
	// off to the S3 restoration bucket.
	RestoreDirectory        string

	// If true, we should restore bags to our partners' test
	// restoration buckets instead of the usual restoration
	// buckets. This should be true only in the demo config,
	// which is what we run on test.aptrust.org. Also note
	// that CustomRestoreBucket overrides this.
	RestoreToTestBuckets    bool

	// Configuration options for apt_restore
	RestoreWorker           WorkerConfig

	// SkipAlreadyProcessed indicates whether or not the
	// bucket_reader should  put successfully-processed items into
	// NSQ for re-processing. This is amost always set to false.
	// The exception is when we deliberately want to reprocess
	// items to test code changes.
	SkipAlreadyProcessed    bool

	// Configuration options for apt_store
	StoreWorker             WorkerConfig

	// TarDirectory is the directory in which we will
	// untar files from S3. This should be on a volume
	// with lots of free disk space.
	TarDirectory            string

	// Configuration options for apt_trouble
	TroubleWorker           WorkerConfig

}

// Ensures that the logging directory exists, creating it if necessary.
// Returns the absolute path the logging directory.
func (config *Config) EnsureLogDirectory() (string, error) {
	config.ExpandFilePaths()
	err := config.createDirectories()
	if err != nil {
		return "", err
	}
	return config.AbsLogDirectory(), nil
}

func (config *Config) AbsLogDirectory() (string) {
	absLogDir, err := filepath.Abs(config.LogDirectory)
	if err != nil {
		msg := fmt.Sprintf("Cannot get absolute path to log directory. "+
			"config.LogDirectory is set to '%s'", config.LogDirectory)
		panic(msg)
	}
	return absLogDir
}

// This returns the configuration that the user requested.
// If the user did not specify any configuration (using the
// -config flag), or if the specified configuration cannot
// be found, this prints a help message and terminates the
// program. Param pathToConfig file should be a path relative
// to EXCHANGE_HOME. Param requestedConfig should be "dev",
// "demo", "test" or some other config environment name
// defined in the config file.
func Load(pathToConfigFile, requestedConfig string) (config Config, err error) {
	configurations, err := loadConfigFile(pathToConfigFile)
	if err != nil {
		return Config{}, err
	}
	config, configExists := configurations[requestedConfig]
	if requestedConfig == "" || !configExists {
		configNames := availableConfigNames(configurations)
		detailedError := fmt.Errorf("Unrecognized config '%s'. " +
			"Please specify one of the following configurations: %s",
			requestedConfig, strings.Join(configNames, ","))
		return Config{}, detailedError
	}
	config.ActiveConfig = requestedConfig
	config.ExpandFilePaths()
	err = config.createDirectories()
	if err != nil {
		return Config{}, err
	}
	return config, nil
}

func availableConfigNames(configurations map[string]Config) []string {
	names := make([]string, len(configurations))
	i := 0
	for name, _ := range configurations {
		names[i] = name
		i++
	}
	return names
}

// This function reads the config.json file and returns a map of
// available configurations.
func loadConfigFile(pathToConfigFile string) (configurations map[string]Config, err error) {
	if pathToConfigFile == "" {
		filepath.Join("config", "config.json")
	}
	file, err := fileutil.LoadRelativeFile(pathToConfigFile)
	if err != nil {
		detailedError := fmt.Errorf("Error reading config file '%s': %v\n",
			pathToConfigFile, err)
		return nil, detailedError
	}
	err = json.Unmarshal(file, &configurations)
	if err != nil {
		detailedError := fmt.Errorf("Error parsing JSON from config file '%s':",
			pathToConfigFile, err)
		return nil, detailedError
	}
	return configurations, nil
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

// Expands ~ file paths
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
	expanded, err = fileutil.ExpandTilde(config.DPNStagingDirectory)
	if err == nil {
		config.DPNStagingDirectory = expanded
	}
	expanded, err = fileutil.ExpandTilde(config.DPNHomeDirectory)
	if err == nil {
		config.DPNHomeDirectory = expanded
	}
}

func (config *Config) createDirectories() (error) {
	if config.TarDirectory == "" {
		return fmt.Errorf("You must defined config.TarDirectory")
	}
	if config.LogDirectory == "" {
		return fmt.Errorf("You must defined config.LogDirectory")
	}
	if config.RestoreDirectory == "" {
		return fmt.Errorf("You must defined config.RestoreDirectory")
	}
	if config.ReplicationDirectory == "" {
		return fmt.Errorf("You must defined config.ReplicationDirectory")
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
	return nil
}
