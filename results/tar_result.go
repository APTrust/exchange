package results

import (
	"github.com/APTrust/exchange/models"
)

// TarResult contains information about the attempt to untar
// a bag.
type TarResult struct {
	InputFile     string
	OutputDir     string
	Warnings      []string
	FilesUnpacked []string
	LocalFiles    []*models.LocalFile
	Summary       *Summary
}

// Returns true if any of the untarred files are new or updated.
func (result *TarResult) AnyFilesNeedSaving() (bool) {
	for _, file := range result.LocalFiles {
		if file.NeedsSave == true {
			return true
		}
	}
	return false
}

// FilePaths returns a list of all the File paths
// that were untarred from the bag. The list will look something
// like "data/file1.gif", "data/file2.pdf", etc.
func (result *TarResult) FilePaths() []string {
	paths := make([]string, len(result.LocalFiles))
	for index, file := range result.LocalFiles {
		paths[index] = file.Path
	}
	return paths
}

// Returns the File with the specified path, if it exists.
func (result *TarResult) GetFileByPath(filePath string) (*models.LocalFile) {
	for index, file := range result.LocalFiles {
		if file.Path == filePath {
			// Be sure to return to original, and not a copy!
			return result.LocalFiles[index]
		}
	}
	return nil
}

// MergeExistingFiles merges data from generic files that
// already exist in Fedora. This is necessary when an existing
// bag is reprocessed or re-uploaded.
func (result *TarResult) MergeExistingFiles(genericFiles []*models.GenericFile) {
	for _, genericFile := range genericFiles {
		origPath, _ := genericFile.OriginalPath()
		file := result.GetFileByPath(origPath)
		if file != nil {
			file.ExistingFile = true
			// Files have the same path and name. If the checksum
			// has not changed, there is no reason to re-upload
			// this file to the preservation bucket, nor is there
			// any reason to create new ingest events in Fedora.
			existingMd5 := genericFile.GetChecksum("md5")
			if file.Md5 == existingMd5.Digest {
				file.NeedsSave = false
				file.StorageURL = genericFile.URI
				file.StorageMd5 = existingMd5.Digest
				ingestEvents := genericFile.FindEventsByType("ingest")
				if len(ingestEvents) > 0 {
					lastIngest := ingestEvents[len(ingestEvents) - 1]
					file.StoredAt = lastIngest.DateTime
				}
			}
		}
	}
}

// Returns true if any generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AnyFilesCopiedToPreservation() bool {
	for _, file := range result.LocalFiles {
		if file.StorageURL != "" {
			return true
		}
	}
	return false
}

// Returns true if all generic files were successfully copied
// to S3 long term storage.
func (result *TarResult) AllFilesCopiedToPreservation() bool {
	for _, file := range result.LocalFiles {
		if file.NeedsSave && file.StorageURL == "" {
			return false
		}
	}
	return true
}
