package stats

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"io/ioutil"
	"os"
	"regexp"
)

// APTBucketReaderStats records stats for the apt_bucket_reader
// worker. It's not meant for regular production, because it can
// use a lot of member when the bucket reader encounters thousands
// of objects. It's primarily useful for integration tests when we
// want the bucket reader to tell us what it did, so we can compare
// that to what actually happened. It's also useful periodicall in
// production, to diagnose problems in smaller runs.
type APTBucketReaderStats struct {
	InstitutionsCached      []*models.Institution
	WorkItemsCached         []*models.WorkItem
	WorkItemsFetched        []*models.WorkItem
	WorkItemsCreated        []*models.WorkItem
	WorkItemsQueued         []*models.WorkItem
	WorkItemsMarkedAsQueued []*models.WorkItem
	S3Items                 []string
	Errors                  []string
	Warnings                []string
}

// Creates a new, empty APTBucketReaderStats object.
func NewAPTBucketReaderStats() *APTBucketReaderStats {
	return &APTBucketReaderStats{
		InstitutionsCached:      make([]*models.Institution, 0),
		WorkItemsCached:         make([]*models.WorkItem, 0),
		WorkItemsFetched:        make([]*models.WorkItem, 0),
		WorkItemsCreated:        make([]*models.WorkItem, 0),
		WorkItemsQueued:         make([]*models.WorkItem, 0),
		WorkItemsMarkedAsQueued: make([]*models.WorkItem, 0),
		S3Items:                 make([]string, 0),
		Errors:                  make([]string, 0),
		Warnings:                make([]string, 0),
	}
}

// Loads bucket reader stats from a JSON file like the ones that
// APTBucketReaderStats.DumpToFile dumps out.
func APTBucketReaderStatsLoadFromFile(pathToFile string) (*APTBucketReaderStats, error) {
	file, err := ioutil.ReadFile(pathToFile)
	if err != nil {
		detailedError := fmt.Errorf("Error reading file '%s': %v\n",
			pathToFile, err)
		return nil, detailedError
	}
	_stats := &APTBucketReaderStats{}
	err = json.Unmarshal(file, _stats)
	if err != nil {
		detailedError := fmt.Errorf("Error parsing JSON from file '%s':",
			pathToFile, err)
		return nil, detailedError
	}
	return _stats, nil
}

// Dumps a JSON representation of this object to a file at the specified
// path. This will overwrite the existing file, if the existing file has
// a .json extension. Note that converting the stats object to JSON can
// use a lot of memory, if you're working with a lot of data. This is safe
// for integration testing, and it dumps out human-readable formatted JSON.
// See also APTBucketReaderStatsLoadFromFile.
func (stats *APTBucketReaderStats) DumpToFile(pathToFile string) error {
	// Matches .json, or tempfile with random ending, like .json43272
	fileNameLooksSafe, err := regexp.MatchString("\\.json\\d*$", pathToFile)
	if err != nil {
		return fmt.Errorf("DumpToFile(): path '%s'?? : %v", pathToFile, err)
	}
	if fileutil.FileExists(pathToFile) && !fileNameLooksSafe {
		return fmt.Errorf("DumpToFile() will not overwrite existing file "+
			"'%s' because that might be dangerous. Give your output file a .json "+
			"extension to be safe.", pathToFile)
	}

	jsonData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}

	outputFile, err := os.Create(pathToFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	outputFile.Write(jsonData)
	return nil
}

// Adds an institution to the list of cached institutions.
func (stats *APTBucketReaderStats) AddToInstitutionsCached(inst *models.Institution) {
	stats.InstitutionsCached = append(stats.InstitutionsCached, inst)
}

// Returns true if the Institution with the specified identifier is in
// the Institutions cache.
func (stats *APTBucketReaderStats) InstitutionsCachedContains(identifier string) bool {
	return stats.InstitutionByIdentifier(identifier) != nil
}

// Finds an Institution in the cache by identifier. Returns nil if not found.
func (stats *APTBucketReaderStats) InstitutionByIdentifier(identifier string) *models.Institution {
	var matchingInst *models.Institution
	for _, inst := range stats.InstitutionsCached {
		if inst.Identifier == identifier {
			matchingInst = inst
			break
		}
	}
	return matchingInst
}

// Adds a WorkItem to a list.
func (stats *APTBucketReaderStats) AddWorkItem(listName string, item *models.WorkItem) error {
	if listName == "WorkItemsCached" {
		stats.WorkItemsCached = append(stats.WorkItemsCached, item)
	} else if listName == "WorkItemsFetched" {
		stats.WorkItemsFetched = append(stats.WorkItemsFetched, item)
	} else if listName == "WorkItemsCreated" {
		stats.WorkItemsCreated = append(stats.WorkItemsCreated, item)
	} else if listName == "WorkItemsQueued" {
		stats.WorkItemsQueued = append(stats.WorkItemsQueued, item)
	} else if listName == "WorkItemsMarkedAsQueued" {
		stats.WorkItemsMarkedAsQueued = append(stats.WorkItemsMarkedAsQueued, item)
	} else {
		return fmt.Errorf("No list called %s", listName)
	}
	return nil
}

// Does what it says.
func (stats *APTBucketReaderStats) FindWorkItemByNameAndEtag(listName, name, etag string) (*models.WorkItem, error) {
	var list []*models.WorkItem
	if listName == "WorkItemsCached" {
		list = stats.WorkItemsCached
	} else if listName == "WorkItemsFetched" {
		list = stats.WorkItemsFetched
	} else if listName == "WorkItemsCreated" {
		list = stats.WorkItemsCreated
	} else if listName == "WorkItemsQueued" {
		list = stats.WorkItemsQueued
	} else if listName == "WorkItemsMarkedAsQueued" {
		list = stats.WorkItemsMarkedAsQueued
	} else {
		return nil, fmt.Errorf("No list called %s", listName)
	}
	for _, item := range list {
		if item.Name == name && item.ETag == etag {
			return item, nil
		}
	}
	return nil, nil
}

// Returns the WorkItem with the matching ID, or nil.
func (stats *APTBucketReaderStats) FindWorkItemById(listName string, id int) (*models.WorkItem, error) {
	var list []*models.WorkItem
	if listName == "WorkItemsCached" {
		list = stats.WorkItemsCached
	} else if listName == "WorkItemsFetched" {
		list = stats.WorkItemsFetched
	} else if listName == "WorkItemsCreated" {
		list = stats.WorkItemsCreated
	} else if listName == "WorkItemsQueued" {
		list = stats.WorkItemsQueued
	} else if listName == "WorkItemsMarkedAsQueued" {
		list = stats.WorkItemsMarkedAsQueued
	} else {
		return nil, fmt.Errorf("No list called %s", listName)
	}
	for _, item := range list {
		if item.Id == id {
			return item, nil
		}
	}
	return nil, nil
}

// Adds an item to the list of files that the bucket reader found in the S3
// receiving buckets. Param bucketAndKey should be something like
// "aptrust.receiving.virginia.edu/virginia.edu_12345678.tar"
func (stats *APTBucketReaderStats) AddS3Item(bucketAndKey string) {
	stats.S3Items = append(stats.S3Items, bucketAndKey)
}

// Returns true if the specified bucketAndKey was found in S3
func (stats *APTBucketReaderStats) S3ItemWasFound(bucketAndKey string) bool {
	return util.StringListContains(stats.S3Items, bucketAndKey)
}

// Adds an error message to the stats.
func (stats *APTBucketReaderStats) AddError(message string) {
	stats.Errors = append(stats.Errors, message)
}

// Returns true if this object contains any errors
func (stats *APTBucketReaderStats) HasErrors() bool {
	return len(stats.Errors) > 0
}

// Adds a warning to the stats.
func (stats *APTBucketReaderStats) AddWarning(message string) {
	stats.Warnings = append(stats.Warnings, message)
}

// Returns true if this object contains any warnings
func (stats *APTBucketReaderStats) HasWarnings() bool {
	return len(stats.Warnings) > 0
}

// Returns the WorkItem with the matching name and etag, or nil.
func (stats *APTBucketReaderStats) findWorkItemByNameAndEtag(workItemList []*models.WorkItem, name, etag string) *models.WorkItem {
	for _, item := range workItemList {
		if item.Name == name && item.ETag == etag {
			return item
		}
	}
	return nil
}
