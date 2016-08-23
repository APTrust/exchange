package pharos_requests_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/pharos_requests"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkItemByEtagNameAndDate(t *testing.T) {
	name := "virginia.edu.12345678"
	etag := "987654321"
	date := time.Now().UTC()
	values := pharos_requests.WorkItemByNameEtagDate(name, etag, date)
	assert.Equal(t, name, values.Get("name"))
	assert.Equal(t, etag, values.Get("etag"))
	assert.Equal(t, date.Format(time.RFC3339), values.Get("bag_date"))
}

func TestIngestWorkItems(t *testing.T) {
	values := pharos_requests.IngestWorkItems()
	assert.Equal(t, constants.ActionIngest, values.Get("action"))
	assert.Equal(t, constants.StageReceive, values.Get("stage"))
	assert.Equal(t, constants.StatusPending, values.Get("status"))
	assert.Equal(t, "true", values.Get("retry"))
	assert.Equal(t, "", values.Get("queued_at"))
}

func TestRestoreWorkItems(t *testing.T) {
	values := pharos_requests.RestoreWorkItems()
	assert.Equal(t, constants.ActionRestore, values.Get("action"))
	assert.Equal(t, constants.StageRequested, values.Get("stage"))
	assert.Equal(t, constants.StatusPending, values.Get("status"))
	assert.Equal(t, "true", values.Get("retry"))
	assert.Equal(t, "", values.Get("queued_at"))
}

func TestDeleteWorkItems(t *testing.T) {
	values := pharos_requests.DeleteWorkItems()
	assert.Equal(t, constants.ActionDelete, values.Get("action"))
	assert.Equal(t, constants.StageRequested, values.Get("stage"))
	assert.Equal(t, constants.StatusPending, values.Get("status"))
	assert.Equal(t, "true", values.Get("retry"))
	assert.Equal(t, "", values.Get("queued_at"))
}

func TestDPNIngestWorkItems(t *testing.T) {
	values := pharos_requests.DPNIngestWorkItems()
	assert.Equal(t, constants.ActionDPN, values.Get("action"))
	assert.Equal(t, constants.StageRequested, values.Get("stage"))
	assert.Equal(t, constants.StatusPending, values.Get("status"))
	assert.Equal(t, "true", values.Get("retry"))
	assert.Equal(t, "", values.Get("queued_at"))
}
