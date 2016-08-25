package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)

var _context *context.Context
var institutions map[string]*models.Institution
var recentIngestItems map[string]int

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	institutions = make(map[string]*models.Institution)
	recentIngestItems = make(map[string]int)
	_context = context.NewContext(config)
	cacheInstitutions()
	cacheRecentIngestItems()
	readAllBuckets()
}

// Cache a list of all institutions. There are < 20.
// Exit on failure.
func cacheInstitutions() {
	// from Pharos client
	// key = identifier, value = institution
	// Die on error
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "100")
	resp := _context.PharosClient.InstitutionList(params)
	dieOnBadResponse("Can't get institutions list.", resp)
	for _, inst := range resp.Institutions() {
		institutions[inst.Identifier] = inst
	}
	_context.MessageLog.Info("Loaded %d institutions", len(institutions))
}

// Cache a list of Ingest items that have been added to
// the list of WorkItems in the past 24 hours, so we won't
// have to do 1000 lookups.
// Exit on failure.
func cacheRecentIngestItems() {
	// From Pharos client
	// Should have a policy in config:
	// cache items where created_at <= 24 hours, or some such
	// Can probably use key = name+etag+date, value = WorkItemId
    // Die on error
	twentyFourHoursAgo := time.Now().Add(-24 * time.Hour).UTC()
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "100")
	params.Add("item_action", "ingest")
	params.Add("created_after", twentyFourHoursAgo.Format(time.RFC3339))
	hasMoreResults := true
	for hasMoreResults {
		resp := _context.PharosClient.WorkItemList(params)
		msg := fmt.Sprintf("Can't get page %s of WorkItem list.", params.Get("page"))
		dieOnBadResponse(msg, resp)
		for _, workItem := range resp.WorkItems() {
			key := fmt.Sprintf("%s|%s|%s",workItem.Name, workItem.ETag,
				workItem.BagDate.Format(time.RFC3339))
			recentIngestItems[key] = workItem.Id
		}
		if resp.Next != nil {
			pageNum, err := strconv.Atoi(params.Get("page"))
			if err != nil {
				msg := fmt.Sprintf("Aargh! %s don't look like no number!", params.Get("page"))
				die(msg)
			}
			params.Set("page", strconv.Itoa(pageNum + 1))
		} else {
			hasMoreResults = false
		}
	}
	_context.MessageLog.Info("Loaded %d recent ingest WorkItems", len(recentIngestItems))
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

// See if you can figure out from the function name what this does.
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

// Tell the user about the program.
func printUsage() {
	message := `
apt_bucket_reader: Reads the contents of S3 receiving buckets, and creates
WorkItem entries and NSQ entries for bags awaiting ingest in those buckets.

Usage: apt_bucket_reader -config=<absolute path to APTrust config file>
`
	fmt.Println(message)
}

func dieOnBadResponse(message string, resp *network.PharosResponse) {
	if resp.Error != nil || resp.Response.StatusCode != http.StatusOK {
		respData, _ := resp.RawResponseData()
		detailedMessage := fmt.Sprintf(
			"URL: %s \n" +
			"Message: %s \n" +
			"Error: %s \n" +
			"Raw response: %s",
			resp.Request.URL, message,
			resp.Error.Error(), string(respData))
		die(detailedMessage)
	}
}


// Print an error message to STDERR (and the log, if possible),
// and then exit with a code indicating error.
func die(message string) {
	fmt.Fprintf(os.Stderr, "%s\n", message)
	if _context != nil && _context.MessageLog != nil {
		_context.MessageLog.Fatal(message,
			"\n\nSTACK TRACE:\n\n",
			string(debug.Stack()))
	}
	os.Exit(1)
}
