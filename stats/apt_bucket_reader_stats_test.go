package stats_test

import (
	"fmt"
	"github.com/APTrust/exchange/stats"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

// Create a stats object with 5 of everything, no errors and no warnings.
func makeAPTBucketReaderStats() (*stats.APTBucketReaderStats) {
	_stats := stats.NewAPTBucketReaderStats()
	for i := 1; i <= 5; i++ {
		inst := testutil.MakeInstitution()
		inst.Identifier = fmt.Sprintf("inst_%d", i)
		_stats.AddToInstitutionsCached(inst)

		workItem := testutil.MakeWorkItem()
		workItem.Id = i
		workItem.Name = fmt.Sprintf("item_%d.tar", i)
		workItem.ETag = fmt.Sprintf("etag_%d", i)
		_stats.AddWorkItem("WorkItemsCached", workItem)
		_stats.AddWorkItem("WorkItemsFetched", workItem)
		_stats.AddWorkItem("WorkItemsCreated", workItem)
		_stats.AddWorkItem("WorkItemsQueued", workItem)
		_stats.AddWorkItem("WorkItemsMarkedAsQueued", workItem)
		_stats.AddS3Item(fmt.Sprintf("test.edu/test_item_%d", i))
	}
	return _stats
}

func TestNewAPTBucketReaderStats(t *testing.T) {
	_stats := stats.NewAPTBucketReaderStats()
	require.NotNil(t, _stats)
	assert.NotNil(t, _stats.InstitutionsCached)
	assert.NotNil(t, _stats.WorkItemsCached)
	assert.NotNil(t, _stats.WorkItemsFetched)
	assert.NotNil(t, _stats.WorkItemsCreated)
	assert.NotNil(t, _stats.WorkItemsQueued)
	assert.NotNil(t, _stats.WorkItemsMarkedAsQueued)
	assert.NotNil(t, _stats.S3Items)
	assert.NotNil(t, _stats.Errors)
	assert.NotNil(t, _stats.Warnings)
}

func TestBucket_AddToInstitutionsCached(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	inst := testutil.MakeInstitution()
	_stats.AddToInstitutionsCached(inst)
	lastInst := _stats.InstitutionsCached[len(_stats.InstitutionsCached) -1]
	assert.Equal(t, inst.Identifier, lastInst.Identifier)
}

func TestBucket_InstitutionsCachedContains(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	assert.True(t, _stats.InstitutionsCachedContains("inst_2"))
	assert.False(t, _stats.InstitutionsCachedContains("inst_2000"))
}

func TestBucket_InstitutionByIdentifier(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	assert.True(t, _stats.InstitutionsCachedContains("inst_2"))
	assert.False(t, _stats.InstitutionsCachedContains("inst_2000"))
}

func TestBucket_AddToWorkItemsCached(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("WorkItemsCached", workItem)
	lastWorkItem := _stats.WorkItemsCached[len(_stats.WorkItemsCached) - 1]
	assert.Equal(t, workItem.Name, lastWorkItem.Name)
	assert.Equal(t, workItem.ETag, lastWorkItem.ETag)
}

func TestBucket_WorkItemCacheFindByNameAndEtag(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCached", "item_1.tar", "etag_1")
	item2, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCached", "item_2.tar", "etag_2")
	noSuchItem, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCached", "nosuchitem.tar", "etag_none")
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, "item_1.tar", item1.Name)
	assert.Equal(t, "etag_1", item1.ETag)
	assert.Equal(t, "item_2.tar", item2.Name)
	assert.Equal(t, "etag_2", item2.ETag)
}

func TestBucket_WorkItemCacheFindById(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemById("WorkItemsCached", 1)
	item2, _ := _stats.FindWorkItemById("WorkItemsCached", 2)
	noSuchItem, _ := _stats.FindWorkItemById("WorkItemsCached", 351)
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, 1, item1.Id)
	assert.Equal(t, 2, item2.Id)
}

func TestBucket_AddToWorkItemsFetched(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("WorkItemsFetched", workItem)
	lastWorkItem := _stats.WorkItemsFetched[len(_stats.WorkItemsFetched) -1]
	assert.Equal(t, workItem.Name, lastWorkItem.Name)
	assert.Equal(t, workItem.ETag, lastWorkItem.ETag)
}

func TestBucket_WorkItemFetchedFindByNameAndEtag(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsFetched", "item_1.tar", "etag_1")
	item2, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsFetched", "item_2.tar", "etag_2")
	noSuchItem, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsFetched", "nosuchitem.tar", "etag_none")
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, "item_1.tar", item1.Name)
	assert.Equal(t, "etag_1", item1.ETag)
	assert.Equal(t, "item_2.tar", item2.Name)
	assert.Equal(t, "etag_2", item2.ETag)
}

func TestBucket_WorkItemFetchedFindById(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemById("WorkItemsFetched", 1)
	item2, _ := _stats.FindWorkItemById("WorkItemsFetched", 2)
	noSuchItem, _ := _stats.FindWorkItemById("WorkItemsFetched", 351)
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, 1, item1.Id)
	assert.Equal(t, 2, item2.Id)
}

func TestBucket_AddToWorkItemsCreated(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("WorkItemsCreated", workItem)
	lastWorkItem := _stats.WorkItemsCreated[len(_stats.WorkItemsCreated) -1]
	assert.Equal(t, workItem.Name, lastWorkItem.Name)
	assert.Equal(t, workItem.ETag, lastWorkItem.ETag)
}

func TestBucket_WorkItemCreatedFindByNameAndEtag(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCreated", "item_1.tar", "etag_1")
	item2, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCreated", "item_2.tar", "etag_2")
	noSuchItem, _ := _stats.FindWorkItemByNameAndEtag("WorkItemsCreated", "nosuchitem.tar", "etag_none")
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, "item_1.tar", item1.Name)
	assert.Equal(t, "etag_1", item1.ETag)
	assert.Equal(t, "item_2.tar", item2.Name)
	assert.Equal(t, "etag_2", item2.ETag)
}

func TestBucket_WorkItemCreatedFindById(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemById("WorkItemsCreated", 1)
	item2, _ := _stats.FindWorkItemById("WorkItemsCreated", 2)
	noSuchItem, _ := _stats.FindWorkItemById("WorkItemsCreated", 351)
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
	assert.Equal(t, 1, item1.Id)
	assert.Equal(t, 2, item2.Id)
}

func TestBucket_AddToWorkItemsQueued(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("WorkItemsQueued", workItem)
	lastWorkItem := _stats.WorkItemsQueued[len(_stats.WorkItemsQueued) -1]
	assert.Equal(t, workItem.Id, lastWorkItem.Id)
}

func TestBucket_WorkItemWasQueued(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemById("WorkItemsQueued", 1)
	item2, _ := _stats.FindWorkItemById("WorkItemsQueued", 2)
	noSuchItem, _ := _stats.FindWorkItemById("WorkItemsQueued", 300)
	assert.NotNil(t, item1)
	assert.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
}

func TestBucket_AddToWorkItemsMarkedAsQueued(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("WorkItemsMarkedAsQueued", workItem)
	lastWorkItem := _stats.WorkItemsMarkedAsQueued[len(_stats.WorkItemsMarkedAsQueued) -1]
	assert.Equal(t, workItem.Id, lastWorkItem.Id)
}

func TestBucket_WorkItemWasMarkedAsQueued(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	item1, _ := _stats.FindWorkItemById("WorkItemsMarkedAsQueued", 1)
	item2, _ := _stats.FindWorkItemById("WorkItemsMarkedAsQueued", 2)
	noSuchItem, _ := _stats.FindWorkItemById("WorkItemsMarkedAsQueued", 300)
	assert.NotNil(t, item1)
	assert.NotNil(t, item2)
	assert.Nil(t, noSuchItem)
}

func TestBucket_AddS3Item(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	_stats.AddS3Item("test.edu/test_item_555")
	lastItem := _stats.S3Items[len(_stats.S3Items) - 1]
	assert.Equal(t, "test.edu/test_item_555", lastItem)
}

func TestBucket_S3ItemWasFound(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	assert.True(t, _stats.S3ItemWasFound("test.edu/test_item_1"))
	assert.True(t, _stats.S3ItemWasFound("test.edu/test_item_2"))
	assert.False(t, _stats.S3ItemWasFound("test.edu/test_item_999"))
}

func TestBucket_AddError(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	_stats.AddError("Oopsie!")
	lastItem := _stats.Errors[len(_stats.Errors) - 1]
	assert.Equal(t, "Oopsie!", lastItem)
}

func TestBucket_HasErrors(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	assert.False(t, _stats.HasErrors())
	_stats.AddError("Oopsie!")
	assert.True(t, _stats.HasErrors())
}

func TestBucket_AddWarning(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	_stats.AddWarning("Oopsie!")
	lastItem := _stats.Warnings[len(_stats.Warnings) - 1]
	assert.Equal(t, "Oopsie!", lastItem)
}

func TestBucket_HasWarnings(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	assert.False(t, _stats.HasWarnings())
	_stats.AddWarning("Oopsie!")
	assert.True(t, _stats.HasWarnings())
}

func TestBucket_DumpToFile(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	tempfile, err := ioutil.TempFile("", "apt_bucket_reader_stats_test.json")
	require.Nil(t, err)
	defer os.Remove(tempfile.Name())
	err = _stats.DumpToFile(tempfile.Name())
	require.Nil(t, err)
	assert.True(t, fileutil.FileExists(tempfile.Name()))
	tempFileStat, err := tempfile.Stat()
	require.Nil(t, err)
	assert.True(t, tempFileStat.Size() > 1000)
}

func TestBucket_ReadFromFile(t *testing.T) {
	_stats := makeAPTBucketReaderStats()
	tempfile, err := ioutil.TempFile("", "apt_bucket_reader_stats_test.json")
	require.Nil(t, err)
	defer os.Remove(tempfile.Name())
	err = _stats.DumpToFile(tempfile.Name())
	require.Nil(t, err)
	newStats, err := stats.APTBucketReaderStatsLoadFromFile(tempfile.Name())
	require.Nil(t, err)
	assert.Equal(t, 5, len(newStats.InstitutionsCached))
	assert.Equal(t, 5, len(newStats.WorkItemsCached))
	assert.Equal(t, 5, len(newStats.WorkItemsFetched))
	assert.Equal(t, 5, len(newStats.WorkItemsCreated))
	assert.Equal(t, 5, len(newStats.WorkItemsQueued))
	assert.Equal(t, 5, len(newStats.WorkItemsMarkedAsQueued))
	assert.Equal(t, 5, len(newStats.S3Items))
	assert.Equal(t, 0, len(newStats.Errors))
	assert.Equal(t, 0, len(newStats.Warnings))
}
