package models

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// GlacierRestoreState holds information about the state of the Glacier
// restore process. This is serialized to JSON and stored in the
// Pharos WorkItemState table, so any worker picking up this task
// can know what's been done and what work remains. The worker
// apt_glacier_restore_init uses this object to keep track of its work.
//
// Restoring a full APTrust bag from Glacier requires one Glacier
// retrieval initialization request and (later) one S3 GET request
// for each file in the bag. Large bags may contain tens of thousands
// of files, so workers may have to attempt retrieval initialization
// several times before all requests succeed.
type GlacierRestoreState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// WorkItem is the Pharos WorkItem we're processing.
	// Not serialized because the Pharos WorkItem record will be
	// more up-to-date and authoritative.
	WorkItem *WorkItem `json:"-"`
	// WorkSummary contains information about whether/when
	// we requested this object(s) be restored from Glacier.
	WorkSummary *WorkSummary
	// GenericFile is the file to be restored. This will
	// be nil if we're restoring an entire object. This item
	// is not serialized to JSON.
	GenericFile *GenericFile `json:"-"`
	// IntellectualObject is the object to be restored. This will
	// be nil if we're only restoring a single file. This item
	// is not serialized to JSON.
	IntellectualObject *IntellectualObject `json:"-"`
	// Requests are the requests we've made (or need to make)
	// to Glacier to retrieve the objects we need to retrieve.
	Requests []*GlacierRestoreRequest
}

// NewGlacierRestoreState creates a new GlacierRestoreState object.
func NewGlacierRestoreState(message *nsq.Message, workItem *WorkItem) *GlacierRestoreState {
	return &GlacierRestoreState{
		NSQMessage:  message,
		WorkItem:    workItem,
		WorkSummary: NewWorkSummary(),
		Requests:    make([]*GlacierRestoreRequest, 0),
	}
}

// FindRequest returns the GlacierRestoreRequest for the specified
// GenericFile identifier. If it returns nil, we have not yet submitted
// a retrieval request to Glacier for that file. Be sure to check the
// returned GlacierRestoreRequest to see whether RequestAccepted is true.
func (state *GlacierRestoreState) FindRequest(gfIdentifier string) *GlacierRestoreRequest {
	var request *GlacierRestoreRequest
	if state.Requests != nil {
		for _, req := range state.Requests {
			if req.GenericFileIdentifier == gfIdentifier {
				request = req
				break
			}
		}
	}
	return request
}

// GetReport returns a GlacierRequestReport describing what work
// remains to be done, and how long we can expect the items to
// remain in the S3 buckets. Param gfIdentifiers is a slice of
// GenericFile Identifiers.
func (state *GlacierRestoreState) GetReport(gfIdentifiers []string) *GlacierRequestReport {
	report := NewGlacierRequestReport()
	report.FilesRequired = len(gfIdentifiers)
	requests := make(map[string]*GlacierRestoreRequest, len(state.Requests))
	for _, req := range state.Requests {
		requests[req.GenericFileIdentifier] = req
		report.FilesRequested += 1
		if req.RequestAccepted == false {
			report.RequestsNotAccepted = append(report.RequestsNotAccepted, req.GenericFileIdentifier)
		}
		if req.IsAvailableInS3 == false {
			report.FilesNotYetInS3 = append(report.FilesNotYetInS3, req.GenericFileIdentifier)
		}
		if report.EarliestRequest.IsZero() || req.RequestedAt.Before(report.EarliestRequest) {
			report.EarliestRequest = req.RequestedAt
		}
		if report.LatestRequest.IsZero() || req.RequestedAt.After(report.LatestRequest) {
			report.LatestRequest = req.RequestedAt
		}
		if report.EarliestExpiry.IsZero() || req.EstimatedDeletionFromS3.Before(report.EarliestExpiry) {
			if req.RequestAccepted {
				report.EarliestExpiry = req.EstimatedDeletionFromS3
			}
		}
		if report.LatestExpiry.IsZero() || req.EstimatedDeletionFromS3.After(report.LatestExpiry) {
			if req.RequestAccepted {
				report.LatestExpiry = req.EstimatedDeletionFromS3
			}
		}
	}
	for _, identifier := range gfIdentifiers {
		_, wasRequested := requests[identifier]
		if wasRequested == false {
			report.FilesNotRequested = append(report.FilesNotRequested, identifier)
		}
	}
	return report
}

func (state *GlacierRestoreState) GetFileIdentifiers() []string {
	gfIdentifiers := make([]string, 0)
	if state.GenericFile != nil {
		gfIdentifiers = append(gfIdentifiers, state.GenericFile.Identifier)
	} else if state.IntellectualObject != nil {
		gfIdentifiers = make([]string, len(state.IntellectualObject.GenericFiles))
		for i, gf := range state.IntellectualObject.GenericFiles {
			gfIdentifiers[i] = gf.Identifier
		}
	}
	return gfIdentifiers
}

// GlacierRequestReport provides information on whether all Glacier
// files have been requested, which ones still need to be requested,
// and how long the files should remain available in S3.
type GlacierRequestReport struct {
	// FilesRequired is the number of files we need to request
	// from Glacier. When restoring a single file, this will be
	// set to one. When restoring a full IntellectualObject, this
	// we be set to the number of saved, active (non-deleted) files
	// that make up the object.
	FilesRequired int
	// FilesRequested is the number of file retrieval requests
	// we've made to Glacier. Glacier may have rejected some of
	// these requests. See RequestsNotAccepted.
	FilesRequested int
	// FilesNotRequested is a list of GenericFile identifiers that
	// we were supposed to request from Glacier but have not yet
	// requested.
	FilesNotRequested []string
	// RequestsNotAccepted is a list of GenericFile identifiers that
	// we requested from Glacier that were denied (or errored).
	// We should retry these.
	RequestsNotAccepted []string
	// FilesNotYetInS3 contains a list of files which are not yet
	// available in S3, either because we haven't requested their
	// restoration, the request wasn't accepted, or the request
	// hasn't completed.
	FilesNotYetInS3 []string
	// EarliestRequest is the timestamp on the earliest Glacier retrieval
	// request for this job.
	EarliestRequest time.Time
	// LatestRequest is the timestamp on the latest Glacier retrieval
	// request for this job.
	LatestRequest time.Time
	// EarliestExpiry is the approximate earliest date-time at which
	// a restored file will be deleted from S3. Once restored from
	// Glacier, files only stay in S3 for a few days.
	// See APTGlacierRestoreInit.DAYS_TO_KEEP_IN_S3
	EarliestExpiry time.Time
	// LatestExpiry is the approximate latest date-time at which
	// a restored file will be deleted from S3. Once restored from
	// Glacier, files only stay in S3 for a few days.
	// See APTGlacierRestoreInit.DAYS_TO_KEEP_IN_S3
	LatestExpiry time.Time
}

// NewGlacierRequestReport creates a new GlacierRequestReport
func NewGlacierRequestReport() *GlacierRequestReport {
	return &GlacierRequestReport{
		FilesNotRequested:   make([]string, 0),
		RequestsNotAccepted: make([]string, 0),
		FilesNotYetInS3:     make([]string, 0),
	}
}

// AllRetrievalsInitialed returns true if we have initiated the retrieval
// process for all of the files we were supposed to retrieve.
func (report *GlacierRequestReport) AllRetrievalsInitiated() bool {
	return len(report.FilesNotRequested) == 0 && len(report.RequestsNotAccepted) == 0
}

// AllItemsInS3 returns true if all items have been moved from Glacier
// into S3.
func (report *GlacierRequestReport) AllItemsInS3() bool {
	return len(report.FilesNotRequested) == 0 && len(report.FilesNotYetInS3) == 0
}

// GlacierRestoreRequest describes a request to restore a file
// from Glacier to S3.
type GlacierRestoreRequest struct {
	// GenericFileIdentifier is the identifier of the generic
	// file we want to restore.
	GenericFileIdentifier string
	// GlacierBucket is the bucket that contains the item
	// we want to restore.
	GlacierBucket string
	// GlacierKey is the key we want to restore
	// (usually a UUID, for APTrust).
	GlacierKey string
	// RequestAccepted indicates whether Glacier accepted
	// our request to restore this object. This does not mean
	// the request is complete. It can take several hours for
	// AWS to push the file from Glacier to S3. Check the
	// property IsAvailableInS3 to see if AWS has actually
	// completed the request.
	RequestAccepted bool
	// RequestedAt is the timestamp of the last request to
	// restore this object.
	RequestedAt time.Time
	// EstimatedDeletionFromS3 describes approximately when
	// this item should be available at the RestorationURL.
	// This time can vary, depending on what level of Glacier
	// retrieval service we're using. Using the standard service
	// level, this should be about four hours after RequestedAt,
	// if the requests succeeded.
	EstimatedDeletionFromS3 time.Time
	// SomeoneElseRequested will be true if apt_glacier_restore
	// thinks someone else requested retrieval of the object.
	// If this is true, EstimatedDeletionFromS3 may not be
	// reliable, because we don't know when the retrieval
	// request occurred, or with what parameters.
	SomeoneElseRequested bool
	// IsAvailableInS3 describes whether the file has been
	// made available in S3 for download, a process which typically
	// takes 3-5 hours. If RequestAccepted is true and IsAvailableInS3
	// is false, then the request is still in process.
	IsAvailableInS3 bool
	// LastChecked is the date/time we last checked to see whether
	// this file had been retrieved from Glacier in to S3.
	LastChecked time.Time
}
