package models

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
)

// StorageSummary is a lightweight object built from
// IngestState to be passed into a goroutine that saves
// a file to S3 or Glacier. The goroutine fills in information
// about where and when a file was stored before returning.
//  This allows multiple goroutines to save files concurrently
// to S3/Glacier without having to share data.
type StorageSummary struct {
	// StoreResult is a WorkSummary object than will hold information
	// about the attempt to store a file in S3 or Glacier. The goroutines
	// that save files primarily record errors here, using the AddError()
	// method and the ErrorIsFatal property.
	StoreResult *WorkSummary
	// TarFilePath is the path the tar file containing the bag being
	// processed. This should never be empty.
	TarFilePath string
	// UntarredPath is the absolute path to the untarred version of the
	// bag being processed. This will usually be empty, since we
	// process bags while they're still tarred.
	UntarredPath string
	// GenericFile is the file to be saved in S3/Glacier. The storage
	// goroutine will update this object directly.
	GenericFile *GenericFile
	// StorageOption describes where the GenericFile should be stored.
	StorageOptions []StorageOption
}

// NewStorageSummary creates a new StorageSummary object.
// Param gf is the GenericFile to be saved. It cannot be nil.
// Param tarPath is the absolute path the tar file containing
// the bag, and cannot be empty. Param untarredPath is the absolute
// path to the untarred bag, and may be empty, since most bags
// are processed without untarring.
func NewStorageSummary(gf *GenericFile, tarPath, untarredPath string) (*StorageSummary, error) {
	if gf == nil {
		return nil, fmt.Errorf("Param gf cannot be nil")
	}
	if tarPath == "" {
		return nil, fmt.Errorf("Param tarPath cannot be empty")
	}
	storageOptions, err := GetStorageOptions(gf)
	if err != nil {
		return nil, err
	}
	return &StorageSummary{
		StoreResult:    NewWorkSummary(),
		GenericFile:    gf,
		TarFilePath:    tarPath,
		UntarredPath:   untarredPath,
		StorageOptions: storageOptions,
	}, nil
}

// ----------------------------------------------------------------------
// MINIO REFACTOR ...
// ----------------------------------------------------------------------

// NEEDS TEST
func GetStorageOptions(gf *GenericFile) (storageOpts []StorageOption, err error) {
	if gf.StorageOption == constants.StorageStandard {
		s3, err := GetStorageOption("Standard-S3")
		if err != nil {
			return nil, err
		}
		glacier, err := GetStorageOption("Standard-Glacier")
		if err != nil {
			return nil, err
		}
		storageOpts = append(storageOpts, s3)
		storageOpts = append(storageOpts, glacier)
	} else {
		storageOpt, err := GetStorageOption(gf.StorageOption)
		if err != nil {
			return nil, err
		}
		storageOpts = append(storageOpts, storageOpt)
	}
	return storageOpts, err
}
