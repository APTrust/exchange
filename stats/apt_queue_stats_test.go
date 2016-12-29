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
func makeAPTQueueStats() *stats.APTQueueStats {
	_stats := stats.NewAPTQueueStats()
	for i := 1; i <= 5; i++ {
		workItem := testutil.MakeWorkItem()
		workItem.Id = i
		workItem.Name = fmt.Sprintf("item_%d.tar", i)
		workItem.ETag = fmt.Sprintf("etag_%d", i)
		_stats.AddWorkItem("topic_1", workItem)
		_stats.AddWorkItem("topic_2", workItem)
		_stats.AddWorkItem("topic_3", workItem)
		_stats.AddItemMarkedAsQueued(workItem)
	}
	return _stats
}

func TestNewAPTQueueStats(t *testing.T) {
	_stats := stats.NewAPTQueueStats()
	require.NotNil(t, _stats)
	assert.NotNil(t, _stats.ItemsQueued)
	assert.NotNil(t, _stats.ItemsMarkedAsQueued)
	assert.NotNil(t, _stats.Errors)
	assert.NotNil(t, _stats.Warnings)
}

func TestQueue_AddWorkItem(t *testing.T) {
	_stats := makeAPTQueueStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddWorkItem("topic_1", workItem)
	list := _stats.ItemsQueued["topic_1"]
	lastWorkItem := list[len(list)-1]
	assert.Equal(t, workItem.Id, lastWorkItem.Id)
}

func TestQueue_AddItemMarkedAsQueued(t *testing.T) {
	_stats := makeAPTQueueStats()
	workItem := testutil.MakeWorkItem()
	_stats.AddItemMarkedAsQueued(workItem)
	lastWorkItem := _stats.ItemsMarkedAsQueued[len(_stats.ItemsMarkedAsQueued)-1]
	assert.Equal(t, workItem.Id, lastWorkItem.Id)
}

func TestQueue_FindQueuedItemByName(t *testing.T) {
	_stats := makeAPTQueueStats()
	workItem1 := testutil.MakeWorkItem()
	workItem1.Id = 577
	_stats.AddWorkItem("topic_1", workItem1)
	workItem2 := testutil.MakeWorkItem()
	workItem2.Id = 984
	_stats.AddWorkItem("topic_2", workItem2)

	item1, topic1 := _stats.FindQueuedItemByName(workItem1.Name)
	item2, topic2 := _stats.FindQueuedItemByName(workItem2.Name)
	noSuchItem, emptyTopic := _stats.FindQueuedItemByName("Willy Wonka")
	require.Equal(t, workItem1.Id, item1.Id)
	require.Equal(t, workItem1.Name, item1.Name)
	require.Equal(t, "topic_1", topic1)

	require.Equal(t, workItem2.Id, item2.Id)
	require.Equal(t, workItem2.Name, item2.Name)
	require.Equal(t, "topic_2", topic2)

	assert.Nil(t, noSuchItem)
	assert.Empty(t, emptyTopic)
}

func TestQueue_FindMarkedItemByName(t *testing.T) {
	_stats := makeAPTQueueStats()
	workItem1 := testutil.MakeWorkItem()
	workItem1.Id = 212
	_stats.AddItemMarkedAsQueued(workItem1)
	workItem2 := testutil.MakeWorkItem()
	workItem2.Id = 414
	_stats.AddItemMarkedAsQueued(workItem2)

	item1 := _stats.FindMarkedItemByName(workItem1.Name)
	item2 := _stats.FindMarkedItemByName(workItem2.Name)
	noSuchItem := _stats.FindMarkedItemByName("Penelope Pitstop")
	require.Equal(t, workItem1.Id, item1.Id)
	require.Equal(t, workItem1.Name, item1.Name)

	require.Equal(t, workItem2.Id, item2.Id)
	require.Equal(t, workItem2.Name, item2.Name)

	assert.Nil(t, noSuchItem)
}

func TestQueue_AddError(t *testing.T) {
	_stats := makeAPTQueueStats()
	_stats.AddError("Oopsie!")
	lastItem := _stats.Errors[len(_stats.Errors)-1]
	assert.Equal(t, "Oopsie!", lastItem)
}

func TestQueue_HasErrors(t *testing.T) {
	_stats := makeAPTQueueStats()
	assert.False(t, _stats.HasErrors())
	_stats.AddError("Oopsie!")
	assert.True(t, _stats.HasErrors())
}

func TestQueue_AddWarning(t *testing.T) {
	_stats := makeAPTQueueStats()
	_stats.AddWarning("Oopsie!")
	lastItem := _stats.Warnings[len(_stats.Warnings)-1]
	assert.Equal(t, "Oopsie!", lastItem)
}

func TestQueue_HasWarnings(t *testing.T) {
	_stats := makeAPTQueueStats()
	assert.False(t, _stats.HasWarnings())
	_stats.AddWarning("Oopsie!")
	assert.True(t, _stats.HasWarnings())
}

func TestQueue_DumpToFile(t *testing.T) {
	_stats := makeAPTQueueStats()
	tempfile, err := ioutil.TempFile("", "apt_queue_stats_test.json")
	require.Nil(t, err)
	defer os.Remove(tempfile.Name())
	err = _stats.DumpToFile(tempfile.Name())
	require.Nil(t, err)
	assert.True(t, fileutil.FileExists(tempfile.Name()))
	tempFileStat, err := tempfile.Stat()
	require.Nil(t, err)
	assert.True(t, tempFileStat.Size() > 1000)
}

func TestQueue_ReadFromFile(t *testing.T) {
	_stats := makeAPTQueueStats()
	tempfile, err := ioutil.TempFile("", "apt_queue_stats_test.json")
	require.Nil(t, err)
	defer os.Remove(tempfile.Name())
	err = _stats.DumpToFile(tempfile.Name())
	require.Nil(t, err)
	newStats, err := stats.APTQueueStatsLoadFromFile(tempfile.Name())
	require.Nil(t, err)
	require.NotNil(t, newStats.ItemsQueued)
	require.NotNil(t, newStats.ItemsMarkedAsQueued)

	keys := make([]string, 0)
	for key, value := range newStats.ItemsQueued {
		keys = append(keys, key)
		assert.Equal(t, 5, len(value))
	}

	assert.Equal(t, 3, len(keys))
	assert.Equal(t, 5, len(newStats.ItemsMarkedAsQueued))
	assert.Equal(t, 0, len(newStats.Errors))
	assert.Equal(t, 0, len(newStats.Warnings))
}
