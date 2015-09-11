package results

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"strings"
	"time"
)

/*
Retry will be set to true if the attempt to process the file
failed and should be tried again. This would be case, for example,
if the failure was due to a network error. Retry is
set to false if processing failed for some reason that
will not change: for example, if the file cannot be
untarred, checksums were bad, or data files were missing.
If processing succeeded, Retry is irrelevant.
*/
type IngestResult struct {
	Stage         constants.StageType
	S3File        *models.S3File
	S3FetchResult *S3FetchResult
	TarResult     *TarResult
	BagReadResult *BagReadResult
	FedoraResult  *FedoraResult
	Summary       *Summary
}

// IntellectualObject returns an instance of IntellectualObject
// which describes what was unpacked from the bag. The IntellectualObject
// structure matches Fluctus' IntellectualObject model, and can be sent
// directly to Fluctus for recording.
func (result *IngestResult) IntellectualObject() (obj *models.IntellectualObject, err error) {
	accessRights := result.BagReadResult.Access()
	// We probably should not do this correction, but we
	// need to get through test runs with the bad data
	// out partners submitted.
	if accessRights == "consortial" {
		accessRights = "consortia"
	} else if accessRights == "institutional" {
		accessRights = "institution"
	}
	institution := &models.Institution{
		BriefName: util.OwnerOf(result.S3File.BucketName),
	}
	identifier, err := result.S3File.ObjectName()
	if err != nil {
		return nil, err
	}
	files, err := result.GenericFiles()
	if err != nil {
		return nil, err
	}
	obj = &models.IntellectualObject{
		InstitutionId: institution.BriefName,
		Title:         result.BagReadResult.Title(),
		Description:   result.BagReadResult.Description(),
		Identifier:    identifier,
		Access:        accessRights,
		GenericFiles:  files,
	}
	obj.AltIdentifier = result.BagReadResult.AltId()
	return obj, nil
}

// GenericFiles returns a list of GenericFile objects that were found
// in the bag.
func (result *IngestResult) GenericFiles() (files []*models.GenericFile, err error) {
	files = make([]*models.GenericFile, len(result.TarResult.LocalFiles))
	for i, file := range result.TarResult.LocalFiles {
		gfModel, err := file.ToGenericFile()
		if err != nil {
			return nil, err
		}
		files[i] = gfModel
	}
	return files, nil
}


// IngestStatus returns a lightweight WorkItem object suitable for reporting
// to the Fluctus results table, so that APTrust partners can view
// the status of their submitted bags.
func (result *IngestResult) IngestStatus() (workItem *models.WorkItem) {
	workItem = &models.WorkItem{}
	workItem.Date = time.Now().UTC()
	workItem.Action = constants.ActionIngest
	workItem.Name = result.S3File.Key.Key
	bagDate, _ := time.Parse(constants.S3DateFormat, result.S3File.Key.LastModified)
	workItem.BagDate = bagDate
	workItem.Bucket = result.S3File.BucketName
	// Strip the quotes off the ETag
	workItem.ETag = strings.Replace(result.S3File.Key.ETag, "\"", "", 2)
	workItem.Stage = result.Stage
	workItem.Status = constants.StatusPending
	if result.Summary.HasErrors() {
		workItem.Status = constants.StatusStarted // Did not complete this stage
		workItem.Note = result.Summary.AllErrorsAsString()
		// Indicate whether we want to try re-processing this bag.
		// For transient errors (e.g. network problems), we retry.
		// For permanent errors (e.g. invalid bag), we do not retry.
		workItem.Retry = result.Summary.Retry
		if workItem.Retry == false {
			// Only mark an item as failed if we know we're not
			// going to retry it. If we're going to retry it, leave
			// it as "Pending", so that institutional admins
			// cannot delete it from the ProcessedItems list in
			// Fluctus.
			workItem.Status = constants.StatusFailed
		}
	} else {
		workItem.Note = "No problems"
		if result.Stage == "Record" {
			workItem.Status = constants.StatusSuccess
		}
		// If there were no errors, bag was processed sucessfully,
		// and there is no need to retry.
		workItem.Retry = false
	}
	workItem.Institution = util.OwnerOf(result.S3File.BucketName)
	workItem.Outcome = string(workItem.Status)
	return workItem
}
