package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
    "github.com/aws/aws-sdk-go/service/s3"
	"net/url"
	"strconv"
	"time"
)

// If Config.BucketReaderCacheHours isn't set to something
// sensible, cache Ingest WorkItems up to this many hours old.
const DEFAULT_CACHE_HOURS = 24

// How many S3 keys should we fetch in each batch when
// we're getting the contents of a bucket?
const MAX_KEYS = 1000


// APTBucketReader scans APTrust receiving buckets for items
// that need to be ingested. It creates a WorkItem record and
// an NSQ entry for each qualifying bag, and is responsible for
// knowing whether items in the receiving buckets actually need
// to be queued.
type APTBucketReader struct {
	Context                *context.Context
	Institutions           map[string]*models.Institution
	RecentIngestItems      map[string]*models.WorkItem
	stats                  map[string]interface{}
	statsEnabled           bool
}

// Creates a new bucket reader with the given context.
// Param enableStats is generally used for integration tests.
// Enabling stats in production can cause high memory usage,
// so keep that off unless you're trying to diagnose specific problems.
func NewAPTBucketReader(context *context.Context, enableStats bool) (*APTBucketReader){
	reader := &APTBucketReader{
		Context: context,
		Institutions: make(map[string]*models.Institution),
		RecentIngestItems: make(map[string]*models.WorkItem),
		statsEnabled: enableStats,
	}
	if enableStats {
		reader.stats = make(map[string]interface{})
		reader.stats["InstitutionCache"] = make([]*models.Institution, 0)
		reader.stats["WorkItemsCached"] = make([]*models.WorkItem, 0)
		reader.stats["WorkItemsFetched"] = make([]*models.WorkItem, 0)
		reader.stats["WorkItemsCreated"] = make([]*models.WorkItem, 0)
		reader.stats["WorkItemsQueued"] = make([]int, 0)
		reader.stats["WorkItemsMarkedAsQueued"] = make([]int, 0)
		reader.stats["S3Items"] = make([]string, 0)
		reader.stats["Errors"] = make([]string, 0)
		reader.stats["Warnings"] = make([]string, 0)
	}
	return reader
}

func (reader *APTBucketReader) Run() (error) {
	err := reader.cacheInstitutions()
	if err != nil { return err }
	err = reader.cacheRecentIngestItems()
	if err != nil { return err }
	reader.readAllBuckets()
	return nil
}

// Cache a list of all institutions. There are < 20.
// Exit on failure.
func (reader *APTBucketReader) cacheInstitutions() (error) {
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "100")
	resp := reader.Context.PharosClient.InstitutionList(params)
	if resp.Error != nil {
		reader.addStat("Errors", resp.Error.Error())
		return resp.Error
	}
	if resp.Response.StatusCode != 200 {
		return reader.processPharosError(resp)
	}
	for _, inst := range resp.Institutions() {
		reader.Institutions[inst.Identifier] = inst
		reader.addStat("InstitutionCache", inst)
	}
	reader.Context.MessageLog.Info("Loaded %d institutions", len(reader.Institutions))
	return nil
}

// Cache a list of Ingest items that have been added to
// the list of WorkItems in the past X hours, so we won't
// have to do 1000 lookups. (X is usually around 24, but
// check Config.BucketReaderCacheHours for the exact value.
// This function exits on failure.
func (reader *APTBucketReader) cacheRecentIngestItems() (error) {
	hours := reader.Context.Config.BucketReaderCacheHours
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
		resp := reader.Context.PharosClient.WorkItemList(params)
		if resp.Error != nil {
			reader.addStat("Errors", resp.Error.Error())
			return resp.Error
		}
		if resp.Response.StatusCode != 200 {
			return reader.processPharosError(resp)
		}
		reader.Context.MessageLog.Debug("%s", resp.Request.URL.String())
		for _, workItem := range resp.WorkItems() {
			hashKey := reader.makeHashKey(workItem.Name, workItem.ETag,
				workItem.BagDate.Format(time.RFC3339))
			reader.RecentIngestItems[hashKey] = workItem
			reader.addStat("WorkItemsCached", workItem)
		}
		if resp.Next != nil {
			pageNum, err := strconv.Atoi(params.Get("page"))
			if err != nil {
				msg := fmt.Sprintf("Page number '%s' doesn't look like a number!", params.Get("page"))
				reader.addStat("Errors", msg)
				return fmt.Errorf(msg)
			}
			params.Set("page", strconv.Itoa(pageNum + 1))
		} else {
			hasMoreResults = false
		}
	}
	reader.Context.MessageLog.Info("Loaded %d recent ingest WorkItems", len(reader.RecentIngestItems))
	return nil
}

func (reader *APTBucketReader) makeHashKey(key, etag, lastModified string) (string) {
	return fmt.Sprintf("%s|%s|%s", key, etag, lastModified)
}

func (reader *APTBucketReader) readAllBuckets() {
	for _, bucketName := range reader.Context.Config.ReceivingBuckets {
		reader.processBucket(bucketName)
	}
}

func (reader *APTBucketReader) processBucket(bucketName string) () {
	reader.Context.MessageLog.Debug("Checking bucket %s", bucketName)
	s3ObjList := network.NewS3ObjectList(reader.Context.Config.APTrustS3Region,
		bucketName, MAX_KEYS)
	keepFetching := true
	for keepFetching {
		s3ObjList.GetList()
		if s3ObjList.ErrorMessage != "" {
			reader.addStat("Errors", s3ObjList.ErrorMessage)
			reader.Context.MessageLog.Error(s3ObjList.ErrorMessage)
			break
		}
		for _, s3Object := range s3ObjList.Response.Contents {
			reader.processS3Object(s3Object, bucketName)
		}
		keepFetching = *s3ObjList.Response.IsTruncated
	}
}

func (reader *APTBucketReader) processS3Object (s3Object *s3.Object, bucketName string) {
	if reader.Context.Config.MaxFileSize > int64(0) && *s3Object.Size > reader.Context.Config.MaxFileSize {
		msg := fmt.Sprintf("Skipping %s/%s because size %d is greater than " +
			"current max file size %d", bucketName, *s3Object.Key, *s3Object.Size,
			reader.Context.Config.MaxFileSize)
		reader.addStat("Warnings", msg)
		reader.Context.MessageLog.Debug(msg)
		return
	}
	workItem, err := reader.findWorkItem(bucketName, *s3Object.Key, *s3Object.LastModified)
	if err != nil {
		// Don't create a work item, because one may already exist.
		// Error will be logged and added to stats at source.
		msg := fmt.Sprintf("Not creating WorkItem for %s/%s because " +
			"we can't tell if one already exists.", bucketName, *s3Object.Key)
		reader.Context.MessageLog.Warning(msg)
		reader.addStat("Warnings", msg)
		return
	}
	if workItem == nil {
		workItem = reader.createWorkItem(bucketName, *s3Object.Key, *s3Object.ETag, *s3Object.LastModified)
		if workItem == nil {
			// Error logged and statted at source.
			return
		}
	}
	if workItem.QueuedAt == nil || workItem.QueuedAt.IsZero() {
		reader.addToNSQ(workItem)
		reader.markAsQueued(workItem)
	}
}

func (reader *APTBucketReader) findWorkItem(key, etag string, lastModified time.Time) (*models.WorkItem, error) {
	hashKey := reader.makeHashKey(key, etag, lastModified.Format(time.RFC3339))
	if workItem, ok := reader.RecentIngestItems[hashKey]; ok {
		reader.Context.MessageLog.Debug("Found hash key '%s' in cache", hashKey)
		return workItem, nil
	}
	reader.Context.MessageLog.Debug("Looking up hash key '%s' in Pharos", hashKey)
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "1")
	params.Add("item_action", constants.ActionIngest)
	params.Add("name", key)
	params.Add("etag", etag)
	params.Add("bag_date", lastModified.Format(time.RFC3339))
	resp := reader.Context.PharosClient.WorkItemList(params)
	if resp.Error != nil {
		errMsg := fmt.Sprintf("Error getting WorkItem for name '%s', etag '%s', time '%s': %v",
			params.Get("name"), params.Get("etag"), params.Get("bag_date"), resp.Error)
		reader.Context.MessageLog.Debug("%s", resp.Request.URL.String())
		reader.Context.MessageLog.Error(errMsg)
		reader.addStat("Errors", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if resp.Response.StatusCode != 200 {
		err := reader.processPharosError(resp)
		return nil, err
	}
	workItem := resp.WorkItem()
	if workItem != nil {
		reader.Context.MessageLog.Debug("Found WorkItem for hash key '%s' in Pharos", hashKey)
		reader.addStat("WorkItemsFetched", workItem)
	} else {
		reader.Context.MessageLog.Debug("Did not find WorkItem for hash key '%s' in Pharos", hashKey)
	}
	return workItem, nil
}

func (reader *APTBucketReader) createWorkItem(bucket, key, etag string, lastModified time.Time) (*models.WorkItem) {
	// Create a WorkItem in Pharos
	institution := reader.Institutions[util.OwnerOf(bucket)]
	if institution == nil {
		errMsg := fmt.Sprintf("Cannot find institution record for item %s/%s. " +
			"Owner computes to '%s'", bucket, key, util.OwnerOf(bucket))
		reader.Context.MessageLog.Error(errMsg)
		reader.addStat("Errors", errMsg)
		return nil
	}
	workItem := &models.WorkItem{}
	workItem.Name = key
	workItem.Bucket = bucket
	workItem.ETag = etag
	workItem.BagDate = lastModified
	workItem.InstitutionId = institution.Id
	workItem.User = constants.APTrustSystemUser
	workItem.Date = time.Now().UTC()
	workItem.Note = "Bag is in receiving bucket"
	workItem.Action = constants.ActionIngest
	workItem.Stage = constants.StageReceive
	workItem.Status = constants.StatusPending
	workItem.Outcome = "Item is pending ingest"
	workItem.Retry = true
	resp := reader.Context.PharosClient.WorkItemSave(workItem)

	if resp.Error != nil {
		errMsg := fmt.Sprintf("Error creating WorkItem for name '%s', etag '%s', time '%s': %v",
			workItem.Name, workItem.ETag, workItem.BagDate, resp.Error)
		reader.Context.MessageLog.Debug("%s", resp.Request.URL.String())
		reader.Context.MessageLog.Error(errMsg)
		reader.addStat("Errors", errMsg)
		return nil
	}
	if resp.Response.StatusCode != 200 {
		reader.processPharosError(resp)
		return nil
	}

	savedWorkItem := resp.WorkItem()
	reader.Context.MessageLog.Debug("Created WorkItem with id %d for %s/%s in Pharos",
		savedWorkItem.Id, bucket, key)
	reader.addStat("WorkItemsFetched", savedWorkItem)
	return savedWorkItem
}

func (reader *APTBucketReader) addToNSQ(workItem *models.WorkItem) {
	client := network.NewNSQClient(reader.Context.Config.NsqdHttpAddress)
	err := client.Enqueue("apt_ingest_fetch", workItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error sending WorkItem %d to NSQ: %v", workItem.Id, err)
		reader.addStat("Errors", workItem.Id)
		reader.Context.MessageLog.Error(msg)
		return
	}
	reader.Context.MessageLog.Info("Added WorkItem id %d to NSQ (%s/%s)",
		workItem.Id, workItem.Bucket, workItem.Name)
	reader.addStat("WorkItemsQueued", workItem.Id)
	return
}

func (reader *APTBucketReader) markAsQueued(workItem *models.WorkItem) (*models.WorkItem) {
	utcNow := time.Now().UTC()
	workItem.QueuedAt = &utcNow
	resp := reader.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		errMsg := fmt.Sprintf("Error setting QueuedAt for WorkItem with id %d: %v",
			workItem.Id, resp.Error)
		reader.Context.MessageLog.Debug("%s", resp.Request.URL.String())
		reader.Context.MessageLog.Error(errMsg)
		reader.addStat("Errors", errMsg)
		return nil
	}
	if resp.Response.StatusCode != 200 {
		reader.processPharosError(resp)
		return nil
	}
	reader.addStat("WorkItemsMarkedAsQueued", workItem.Id)
	return resp.WorkItem()
}

func (reader *APTBucketReader) addStat(key string, value interface{}) {
	if reader.statsEnabled {
		// TODO: Fix this!
		// reader.stats[key] = append(reader.stats[key], value)
	}
}

func (reader *APTBucketReader) GetStats() (map[string]interface{}, error) {
	if reader.statsEnabled {
		return reader.stats, nil
	}
	return nil, fmt.Errorf("Stats are not enabled. Use NewATPBucketReader(context, true) next time.")
}

func (reader *APTBucketReader) processPharosError(resp *network.PharosResponse) (error) {
	respBody := ""
	bytesRead, readErr := resp.RawResponseData()
	if readErr == nil {
		respBody = string(bytesRead)
	} else {
		respBody = fmt.Sprintf("[Could not read response body: %v]", readErr)
	}
	msg := fmt.Sprintf("%s %s returned status code %d. Response body: %s",
		resp.Request.Method, resp.Request.URL, resp.Response.StatusCode, respBody)
	reader.Context.MessageLog.Error(msg)
	reader.addStat("Errors", msg)
	return fmt.Errorf(msg)
}
