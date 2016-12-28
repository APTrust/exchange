package stats

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"io/ioutil"
	"os"
	"regexp"
)

// APTQueueStats records information about what apt_queue did.
type APTQueueStats struct {
	ItemsQueued         map[string][]*models.WorkItem
	ItemsMarkedAsQueued []*models.WorkItem
	Errors              []string
	Warnings            []string
}

// NewAPTQueueStats creates a new, empty APTQueueStats object.
func NewAPTQueueStats() *APTQueueStats {
	return &APTQueueStats{
		ItemsQueued:         make(map[string][]*models.WorkItem),
		ItemsMarkedAsQueued: make([]*models.WorkItem, 0),
		Errors:              make([]string, 0),
		Warnings:            make([]string, 0),
	}
}

// APTQueueStatsLoadFromFile loads APTQueueStats from a JSON file.
func APTQueueStatsLoadFromFile(pathToFile string) (*APTQueueStats, error) {
	file, err := ioutil.ReadFile(pathToFile)
	if err != nil {
		detailedError := fmt.Errorf("Error reading file '%s': %v\n",
			pathToFile, err)
		return nil, detailedError
	}
	_stats := &APTQueueStats{}
	err = json.Unmarshal(file, _stats)
	if err != nil {
		detailedError := fmt.Errorf("Error parsing JSON from file '%s': %v",
			pathToFile, err)
		return nil, detailedError
	}
	return _stats, nil
}

// DumpToFile dumps a JSON representation of this object to a file at the specified
// path. This will overwrite the existing file, if the existing file has
// a .json extension. Note that converting the stats object to JSON can
// use a lot of memory, if you're working with a lot of data. This is safe
// for integration testing, and it dumps out human-readable formatted JSON.
// See also APTQueueStatsLoadFromFile.
func (stats *APTQueueStats) DumpToFile(pathToFile string) error {
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

// AddWorkItem adds a WorkItem to the specified topic. This is to record
// the fact that apt_queue added the specified WorkItem id to the specified
// NSQ topic.
func (stats *APTQueueStats) AddWorkItem(topic string, item *models.WorkItem) {
	if stats.ItemsQueued[topic] == nil {
		stats.ItemsQueued[topic] = make([]*models.WorkItem, 0)
	}
	stats.ItemsQueued[topic] = append(stats.ItemsQueued[topic], item)
}

// AddItemMarkedAsQueued adds a WorkItem to the items marked as queued.
// Marked means apt_queue told Pharos that the item has been added to
// the appropriate NSQ topic.
func (stats *APTQueueStats) AddItemMarkedAsQueued(item *models.WorkItem) {
	stats.ItemsMarkedAsQueued = append(stats.ItemsMarkedAsQueued, item)
}

// FindQueuedItemByName returns the queued WorkItem with the specified
// name, or nil. It also returns the name of the topic the WorkItem
// was added to. Name is the bag name as it appears in the receiving bucket,
// with the tar extension, e.g. "my_bag.tar".
func (stats *APTQueueStats) FindQueuedItemByName(name string) (*models.WorkItem, string) {
	for topic, items := range stats.ItemsQueued {
		for _, item := range items {
			if item.Name == name {
				return item, topic
			}
		}
	}
	return nil, ""
}

// FindMarkedItemByName returns the "marked as queued" item with the specified
// name. Name is the bag name as it appears in the receiving bucket, with the
// tar extension, e.g. "my_bag.tar".
func (stats *APTQueueStats) FindMarkedItemByName(name string) *models.WorkItem {
	for _, item := range stats.ItemsMarkedAsQueued {
		if item.Name == name {
			return item
		}
	}
	return nil
}

// Adds an error message to the stats.
func (stats *APTQueueStats) AddError(message string) {
	stats.Errors = append(stats.Errors, message)
}

// Returns true if this object contains any errors
func (stats *APTQueueStats) HasErrors() bool {
	return len(stats.Errors) > 0
}

// Adds a warning to the stats.
func (stats *APTQueueStats) AddWarning(message string) {
	stats.Warnings = append(stats.Warnings, message)
}

// Returns true if this object contains any warnings
func (stats *APTQueueStats) HasWarnings() bool {
	return len(stats.Warnings) > 0
}
