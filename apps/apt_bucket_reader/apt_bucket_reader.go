package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
    "github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)

// If Config.BucketReaderCacheHours isn't set to something
// sensible, cache Ingest WorkItems up to this many hours old.
const DEFAULT_CACHE_HOURS = 24

// How many S3 keys should we fetch in each batch when
// we're getting the contents of a bucket?
const MAX_KEYS = 1000

var _context *context.Context
var institutions map[string]*models.Institution
var recentIngestItems map[string]*models.WorkItem

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	institutions = make(map[string]*models.Institution)
	recentIngestItems = make(map[string]*models.WorkItem)
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
// the list of WorkItems in the past X hours, so we won't
// have to do 1000 lookups. (X is usually around 24, but
// check Config.BucketReaderCacheHours for the exact value.
// This function exits on failure.
func cacheRecentIngestItems() {
	hours := _context.Config.BucketReaderCacheHours
	if hours < 1 {
		hours = DEFAULT_CACHE_HOURS
	}
	createdAfter := time.Now().Add(time.Duration(-1 * hours) * time.Hour).UTC()
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "100")  // change to 4 to test hasMoreResults loop
	params.Add("item_action", constants.ActionIngest)
	params.Add("created_after", createdAfter.Format(time.RFC3339))
	hasMoreResults := true
	for hasMoreResults {
		resp := _context.PharosClient.WorkItemList(params)
		dieMessage := fmt.Sprintf("Can't get page %s of WorkItem list.", params.Get("page"))
		dieOnBadResponse(dieMessage, resp)
		_context.MessageLog.Debug("%s", resp.Request.URL.String())
		for _, workItem := range resp.WorkItems() {
			hashKey := makeHashKey(workItem.Name, workItem.ETag,
				workItem.BagDate.Format(time.RFC3339))
			recentIngestItems[hashKey] = workItem
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

func makeHashKey(key, etag, lastModified string) (string) {
	return fmt.Sprintf("%s|%s|%s", key, etag, lastModified)
}

func readAllBuckets() {
	for _, bucketName := range _context.Config.ReceivingBuckets {
		processBucket(bucketName)
	}
}

func processBucket(bucketName string) () {
	_context.MessageLog.Debug("Checking bucket %s", bucketName)
	s3ObjList := network.NewS3ObjectList(_context.Config.APTrustS3Region,
		bucketName, MAX_KEYS)
	keepFetching := true
	for keepFetching {
		s3ObjList.GetList()
		if s3ObjList.ErrorMessage != "" {
			_context.MessageLog.Error(s3ObjList.ErrorMessage)
			break
		}
		for _, s3Object := range s3ObjList.Response.Contents {
			processS3Object(s3Object, bucketName)
		}
		keepFetching = *s3ObjList.Response.IsTruncated
	}
}

func processS3Object (s3Object *s3.Object, bucketName string) {
	if _context.Config.MaxFileSize > int64(0) && *s3Object.Size > _context.Config.MaxFileSize {
		_context.MessageLog.Debug("Skipping %s/%s because size %d is greater than " +
			"current max file size %d", bucketName, *s3Object.Key, *s3Object.Size,
			_context.Config.MaxFileSize)
		return
	}
	workItem := findWorkItem(bucketName, *s3Object.Key, *s3Object.LastModified)
	if workItem == nil {
		workItem = createWorkItem(bucketName, *s3Object.Key, *s3Object.LastModified)
	}
	// if workItem.QueuedAt.IsZero() {
	// 	addToNSQ(workItem.Id)
	// 	markAsQueued(workItem)
	// }
}

func findWorkItem(key, etag string, lastModified time.Time) (*models.WorkItem) {
	hashKey := makeHashKey(key, etag, lastModified.Format(time.RFC3339))
	if workItem, ok := recentIngestItems[hashKey]; ok {
		_context.MessageLog.Debug("Found hash key '%s' in cache", hashKey)
		return workItem
	}
	_context.MessageLog.Debug("Looking up hash key '%s' in Pharos", hashKey)
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "1")
	params.Add("item_action", constants.ActionIngest)
	params.Add("name", key)
	params.Add("etag", etag)
	params.Add("bag_date", lastModified.Format(time.RFC3339))
	resp := _context.PharosClient.WorkItemList(params)
	dieMessage := fmt.Sprintf("Error getting WorkItem for name '%s', etag '%s', time '%s'",
		params.Get("name"), params.Get("etag"), params.Get("bag_date"))
	dieOnBadResponse(dieMessage, resp)
	_context.MessageLog.Debug("%s", resp.Request.URL.String())
	items := resp.WorkItems()
	if len(items) > 0 {
		_context.MessageLog.Debug("Found WorkItem for hash key '%s' in Pharos", hashKey)
		return items[0]
	}
	_context.MessageLog.Debug("Did not find WorkItem for hash key '%s' in Pharos", hashKey)
	return nil
}

func createWorkItem(key, etag string, lastModified time.Time) (*models.WorkItem) {
	// Create a WorkItem in Pharos
	item := &models.WorkItem{}
	// define it
	// save it
	return item
}

func addToNSQ(workItemId int) {
	// Create NSQ entry and set WorkItem.QueuedAt
	return
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
