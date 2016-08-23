package apt_bucket_reader

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"os"
	"time"
)

var _context *context.Context
var institutions map[string]models.Institution
var recentIngestItems map[string]int

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context = context.NewContext(config)
	cacheInstitutions()
	cacheRecentIngestItems()
}

// Cache a list of all institutions. There are < 20.
func cacheInstitutions() {
	// from Pharos client
	// key = identifier, value = institution
	// Die on error
}

// Cache a list of Ingest items that have been added to
// the list of WorkItems in the past 24 hours, so we won't
// have to do 1000 lookups.
func cacheRecentIngestItems() {
	// From Pharos client
	// Should have a policy in config:
	// cache items where created_at <= 24 hours, or some such
	// Can probably use key = name+etag+date, value = WorkItemId
	// Die on error
}

func readAllBuckets() {
	// for each bucket in _context.Config.ReceivingBuckets
	// ... readBucket(bucket)
}

func readBucket(bucketName string) () {
	// from network.S3ObjectList
	// keep calling GetList until IsTruncated == false
	// foreach item...
	// ...skip if it exceeds max size
	// ...skip if corresponding record exists in cache or Pharos
	// Otherwise--
	// -- create WorkItem in Pharos
	// -- create NSQ entry
	// -- set queued timestamp on WorkItem
}

func createWorkItem(key, etag string, lastModified time.Time) (*models.WorkItem, error) {
	// Create a WorkItem in Pharos
	item := &models.WorkItem{}
	// define it
	// save it
	return item, nil
}

func addToNSQ(workItemId int) (error) {
	// Create NSQ entry and set WorkItem.QueuedAt
	return nil
}

func markAsQueued(workItem *models.WorkItem) (*models.WorkItem, error) {
	// Update WorkItem, so QueuedAt is set
	return workItem, nil
}

func parseCommandLine() (configFile string) {
	var pathToConfigFile string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.Parse()
	if pathToConfigFile == "" {
		printUsage()
		os.Exit(1)
	}
	return pathToConfigFile
}

func printUsage() {
	message := `
apt_bucket_reader: Reads the contents of S3 receiving buckets, and creates
WorkItem entries and NSQ entries for bags awaiting ingest in those buckets.

Usage: apt_bucket_reader -config=<absolute path to APTrust config file>
`
	fmt.Println(message)
}
