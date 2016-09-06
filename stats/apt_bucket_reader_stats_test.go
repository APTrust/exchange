package stats_test

import (
	"fmt"
	"github.com/APTrust/exchange/stats"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Create a stats object with 10 of everything, no errors and no warnings.
func makeAPTBucketReaderStats() (*stats.APTBucketReaderStats) {
	_stats := stats.NewAPTBucketReaderStats()
	for i := 1; i <= 10; i++ {
		inst := testutil.MakeInstitution()
		inst.Identifier = fmt.Sprintf("inst_%d", i)
		_stats.AddToInstitutionsCached(inst)

		workItem := testutil.MakeWorkItem()
		workItem.Id = i
		workItem.Name = fmt.Sprintf("item_%d.tar", i)
		workItem.ETag = fmt.Sprintf("etag_%d", i)
		_stats.AddToWorkItemsCached(workItem)
		_stats.AddToWorkItemsFetched(workItem)
		_stats.AddToWorkItemsCreated(workItem)
		_stats.AddToWorkItemsQueued(workItem.Id)
		_stats.AddToWorkItemsMarkedAsQueued(workItem.Id)
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
