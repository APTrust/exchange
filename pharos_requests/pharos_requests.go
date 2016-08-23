package pharos_requests

import (
	"github.com/APTrust/exchange/constants"
	"net/url"
	"time"
)

// Returns a request for a WorkItem with the matching etag,
// filename and bag date. The bucket_reader uses this to determine
// if an item it finds in a receiving bucket has already been
// added to the WorkItems table.
func WorkItemByNameEtagDate(name, etag string, bagDate time.Time) (url.Values) {
	v := url.Values{}
	v.Add("name", name)
	v.Add("etag", etag)
	v.Add("bag_date", bagDate.Format(time.RFC3339))
	return v
}

// Returns a request for WorkItems that describe APTrust bags sitting
// in receiving buckets, ready to begin the APTrust ingest process,
// that have not yet been added to NSQ.
func IngestWorkItems() (url.Values) {
	v := url.Values{}
	v.Add("action", constants.ActionIngest)
	v.Add("stage", constants.StageReceive)  // Not StageRequested, unlike others
	v.Add("status", constants.StatusPending)
	v.Add("retry", "true")
	v.Add("queued_at", "")
	return v
}

// Returns a request for WorkItems that describe bags users have
// asked us to restore. These items go into the NSQ restore queue.
func RestoreWorkItems() (url.Values) {
	return requestForQueueableItem(constants.ActionRestore)
}

// Returns a request for WorkItems that describe GenericFiles that
// users have asked us to delete. These items go into the NSQ
// delete queue.
func DeleteWorkItems() (url.Values) {
	return requestForQueueableItem(constants.ActionDelete)
}

// Returns a request for WorkItems that describe bags that users
// have asked us to send to DPN. These items go into the NSQ
// DPN package queue.
func DPNIngestWorkItems() (url.Values) {
	return requestForQueueableItem(constants.ActionDPN)
}

func requestForQueueableItem(action string) (url.Values) {
	v := url.Values{}
	v.Add("action", action)
	v.Add("stage", constants.StageRequested)
	v.Add("status", constants.StatusPending)
	v.Add("retry", "true")
	v.Add("queued_at", "")
	return v
}
