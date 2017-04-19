package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"sync"
)

// APTAuditList lists the contents of S3 and Glacier buckets,
// for auditing and other purposes. When the bucket is one of
// the APTrust or DPN long-term preservation buckets, this
// prints extended metadata information for each item it finds.
// For other buckets, it prints standard metadata, such as the
// key name, etag, and size. This prints results to STDOUT, which
// you can then redirect to a file.
type APTAuditList struct {
	context      *context.Context
	region       string
	bucket       string
	keyPrefix    string
	limit        int
	format       string
	csvDelimiter rune
	recordType   int
	concurrency  int
	listClient   *network.S3ObjectList
	headClients  []*network.S3Head
	results      []string
	count        int
	errOccurred  bool
	mutex        *sync.RWMutex
}

const (
	ITEMS_PER_REQUEST = 100
	STORED_FILE       = 1
	DPN_STORED_FILE   = 2
)

// NewAuditList returns a new APTAuditList object.
// Param context is a context.Context object.
// Param region is the AWS S3/Glacier region to connect to.
// Param bucket is the name of the bucket to list.
// Param keyPrefix is the key, or the prefix of the keys,
// you want to look for. A keyPrefix of "abc" will return
// all keys beginning with "abc." An empty keyPrefix
// will return all keys. Param format can be "json", "csv"
// (comma-separated values) or "tsv" (tab-separated values).
// Param limit is the maximum number of keys to return. Set
// limit to zero to return an unlimited number of keys.
// Param concurrency is the number of items to fetch
// simultaneously. It defaults to 4. Max is 32.
func NewAPTAuditList(context *context.Context, region, bucket, keyPrefix, format string, limit, concurrency int) (*APTAuditList, error) {
	if context == nil {
		return nil, fmt.Errorf("Param context cannot be nil")
	}
	if region == "" {
		return nil, fmt.Errorf("Param region cannot be empty")
	}
	if bucket == "" {
		return nil, fmt.Errorf("Param bucket cannot be empty")
	}
	if format != "json" && format != "csv" && format != "tsv" {
		return nil, fmt.Errorf("Param format must be json, csv, or tsv")
	}
	if concurrency > 32 {
		return nil, fmt.Errorf("Param concurrency can be no higher than 32")
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	delimiter := ','
	if format == "tsv" {
		delimiter = '\t'
	}
	recordType := STORED_FILE
	if bucket == context.Config.DPN.DPNPreservationBucket {
		recordType = DPN_STORED_FILE
	}
	return &APTAuditList{
		context:      context,
		region:       region,
		bucket:       bucket,
		keyPrefix:    keyPrefix,
		limit:        limit,
		format:       format,
		csvDelimiter: delimiter,
		recordType:   recordType,
		concurrency:  concurrency,
		results:      make([]string, 0),
		count:        0,
		errOccurred:  false,
		mutex:        &sync.RWMutex{},
	}, nil
}

// Run prints a list of files to STDOUT, and errors to STDERR.
// It returns the number of items listed, and an error if it
// encountered any errors during its run. Check the STDERR log
// for errors if List returns an error.
func (list *APTAuditList) Run() (int, error) {
	listAttempts := 0
	listErrCount := 0
	var err error
	list.initClients()
	for {
		listAttempts += 1
		list.listClient.GetList(list.keyPrefix)
		if list.listClient.ErrorMessage != "" {
			fmt.Fprintln(os.Stderr, list.listClient.ErrorMessage)
			list.flagError()
			listErrCount += 1
			if listAttempts == 3 && listErrCount == 3 {
				break // Config error.
			}
			continue
		}

		// Fetch the records is batches, using concurrent goroutines.
		// The number of goroutines is specified by list.concurrency.
		start := 0
		for {
			start = list.fetchBatch(list.listClient.Response.Contents, start)
			if start >= len(list.listClient.Response.Contents) || list.getCount() >= list.limit {
				break
			}
		}

		if *list.listClient.Response.IsTruncated == false || list.getCount() >= list.limit {
			list.printAll()
			list.clearResults()
			break // no more items to fetch
		}
	}
	return list.getCount(), err
}

// fetchBatch issues a batch of S3 Head requests. The size of the batch
// should be set to list.concurrency. Returns the next start index.
func (list *APTAuditList) fetchBatch(objects []*s3.Object, startIndex int) int {
	howMany := len(list.headClients)
	end := startIndex + howMany
	if end > len(objects) {
		end = len(objects)
	}
	wg := sync.WaitGroup{}
	clientIndex := 0
	for i := startIndex; i < end; i++ {
		obj := list.listClient.Response.Contents[i]
		client := list.headClients[clientIndex]
		clientIndex += 1
		wg.Add(1)
		go func(client *network.S3Head, key string) {
			list.fetchOne(client, key)
			list.incrementCount()
			defer wg.Done()
		}(client, *obj.Key)
	}
	wg.Wait()
	return end
}

// fetchOne fetches a HEAD record for a single key in AWS,
// then adds the record to the list of results.
func (list *APTAuditList) fetchOne(client *network.S3Head, key string) {
	client.Head(key)
	if client.ErrorMessage != "" {
		fmt.Fprintln(os.Stderr, client.ErrorMessage)
		list.flagError()
		return
	}
	strRecord := ""
	var err error
	if list.recordType == DPN_STORED_FILE {
		record := client.DPNStoredFile()
		if list.format == "json" {
			strRecord, err = record.ToJson()
		} else {
			strRecord, err = record.ToCSV(list.csvDelimiter)
		}
	} else {
		record := client.StoredFile()
		if list.format == "json" {
			strRecord, err = record.ToJson()
		} else {
			strRecord, err = record.ToCSV(list.csvDelimiter)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "[Key", key, "]", err.Error())
		list.flagError()
	} else {
		list.addResult(strRecord)
	}
}

// initClients initializes a single S3 list client and number of S3 head
// clients. The number of S3 head clients is based on the concurrency
// setting.
func (list *APTAuditList) initClients() {
	if list.listClient == nil {
		maxKeys := int64(util.Min(list.limit, ITEMS_PER_REQUEST))
		list.listClient = network.NewS3ObjectList(list.region, list.bucket, maxKeys)
		list.headClients = make([]*network.S3Head, list.concurrency)
		for i := 0; i < list.concurrency; i++ {
			list.headClients[i] = network.NewS3Head(list.region, list.bucket)
		}
	}
}

// addResult adds one item to our list of results.
func (list *APTAuditList) addResult(itemRecord string) {
	list.mutex.Lock()
	list.results = append(list.results, itemRecord)
	list.mutex.Unlock()
}

// clearResults clears the list of results.
func (list *APTAuditList) clearResults() {
	list.mutex.Lock()
	list.results = make([]string, 0)
	list.mutex.Unlock()
}

// incrementCount adds one to the count of files listed
func (list *APTAuditList) incrementCount() {
	list.mutex.Lock()
	list.count += 1
	list.mutex.Unlock()
}

// getCount returns the number of items already fetched
func (list *APTAuditList) getCount() int {
	list.mutex.RLock()
	count := list.count
	list.mutex.RUnlock()
	return count
}

// flagError sets a flag saying an error occurred
func (list *APTAuditList) flagError() {
	list.mutex.Lock()
	list.errOccurred = true
	list.mutex.Unlock()
}

// printAll prints the list of results to STDOUT
func (list *APTAuditList) printAll() {
	for _, result := range list.results {
		fmt.Print(result)
	}
}
