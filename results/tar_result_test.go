package results_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/results"
	"github.com/APTrust/exchange/testdata"
	"testing"
	"time"
)

func getTarResult() results.TarResult {
	testdata.InitDateTimes()
    return results.TarResult {
        InputFile: "/mnt/apt_data/ncsu.1840.16-2928.tar",
        OutputDir: "/mnt/apt_data/ncsu.1840.16-2928",
        Warnings: nil,
        FilesUnpacked: []string {
            "aptrust-info.txt",
            "bag-info.txt",
            "bagit.txt",
            "data/ORIGINAL/1",
            "data/ORIGINAL/1-metadata.xml",
            "data/metadata.xml",
            "data/object.properties",
            "manifest-md5.txt",
            "tagmanifest-md5.txt",
        },
        LocalFiles: []*models.LocalFile{
            {
                Path: "data/metadata.xml",
                Size: 5105,
                Created: testdata.TimeZero,
                Modified: testdata.Apr_25_2014,
                Md5: "84586caa94ff719e93b802720501fcc7",
                Md5Verified: testdata.Apr_25_2014,
                Sha256: "ab807222abc85eb3be8c4d5b754c1a5d89d53642d05232f9eade3a539e7f1784",
                Sha256Generated: testdata.June_9_2014,
                Uuid: "b21fdb34-1f79-4101-62c5-56918f4782fc",
                UuidGenerated: testdata.June_9_2014,
                MimeType: "application/xml",
                ErrorMessage: "",
                StorageURL: "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/metadata.xml",
                StoredAt: testdata.July_3_2014,
                StorageMd5: "84586caa94ff719e93b802720501fcc7",
                Identifier: "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml",
                IdentifierAssigned: testdata.Apr_25_2014,
                ExistingFile: false,
                NeedsSave: true,
            },
            {
                Path: "data/object.properties",
                Size: 73,
                Created: testdata.TimeZero,
                Modified: testdata.Apr_25_2014,
                Md5: "a340203a24dcd6f6ca2bc95a4956c65d",
                Md5Verified: testdata.Apr_25_2014,
                Sha256: "54536211e3ad308e8509091a1db393cbcc7fadd4a9b7f434bec8097d149a2039",
                Sha256Generated: testdata.June_9_2014,
                Uuid: "cba60bd7-1d46-4d53-705f-2298e173bbf9",
                UuidGenerated: testdata.June_9_2014,
                MimeType: "text/plain",
                ErrorMessage: "",
                StorageURL: "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/object.properties",
                StoredAt: testdata.July_3_2014,
                StorageMd5: "a340203a24dcd6f6ca2bc95a4956c65d",
                Identifier: "ncsu.edu/ncsu.1840.16-2928/data/object.properties",
                IdentifierAssigned: testdata.Apr_25_2014,
                ExistingFile: false,
                NeedsSave: true,
            },
            {
                Path: "data/ORIGINAL/1",
                Size: 672316,
                Created: testdata.TimeZero,
                Modified: testdata.Apr_25_2014,
                Md5: "71bf1855639c4194c5a6337cc05c2b19",
                Md5Verified: testdata.Apr_25_2014,
                Sha256: "1e03f27bd4056b9082ea645517d3c419cb488ac316b392b344eda73ea3010169",
                Sha256Generated: testdata.June_9_2014,
                Uuid: "18299764-f623-489e-5171-b9f7e37675a1",
                UuidGenerated: testdata.June_9_2014,
                MimeType: "application/pdf",
                ErrorMessage: "",
                StorageURL: "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/ORIGINAL/1",
                StoredAt: testdata.July_3_2014,
                StorageMd5: "71bf1855639c4194c5a6337cc05c2b19",
                Identifier: "ncsu.edu/ncsu.1840.16-2928/data/ORIGINAL/1",
                IdentifierAssigned: testdata.Apr_25_2014,
                ExistingFile: false,
                NeedsSave: true,
            },
            {
                Path: "data/ORIGINAL/1-metadata.xml",
                Size: 128,
                Created: testdata.TimeZero,
                Modified: testdata.Apr_25_2014,
                Md5: "a4d9c67041d961bb8e003fdc2a3b65e8",
                Md5Verified: testdata.Apr_25_2014,
                Sha256: "60eca5faef45a627d0c1e916026ed6cf91ffe911b5f9b136a3dcc7d99e291519",
                Sha256Generated: testdata.June_9_2014,
                Uuid: "59ce7814-cba5-4898-62d2-9541b2b28295",
                UuidGenerated: testdata.June_9_2014,
                MimeType: "application/xml",
                ErrorMessage: "",
                StorageURL: "https://s3.amazonaws.com/aptrust.test.fixtures/ncsu_files/data/ORIGINAL/1-metadata.xml",
                StoredAt: testdata.July_3_2014,
                StorageMd5: "a4d9c67041d961bb8e003fdc2a3b65e8",
                Identifier: "ncsu.edu/ncsu.1840.16-2928/data/ORIGINAL/1-metadata.xml",
                IdentifierAssigned: testdata.Apr_25_2014,
                ExistingFile: false,
                NeedsSave: true,
            },
        },
    }
}


func buildGenericFiles() []*models.GenericFile {
	// Changed file
	md5_1 := &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "TestMd5Digest",
	}
	sha256_1 := &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "TestSha256Digest",
	}
	checksums1 := make([]*models.ChecksumAttribute, 2)
	checksums1[0] = md5_1
	checksums1[1] = sha256_1
	genericFile1 := &models.GenericFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml",
		ChecksumAttributes: checksums1,
	}

	// Existing file, unchanged
	md5_2 := &models.ChecksumAttribute{
		Algorithm: "md5",
		DateTime: time.Now(),
		Digest: "a340203a24dcd6f6ca2bc95a4956c65d",
	}
	sha256_2 := &models.ChecksumAttribute{
		Algorithm: "sha256",
		DateTime: time.Now(),
		Digest: "54536211e3ad308e8509091a1db393cbcc7fadd4a9b7f434bec8097d149a2039",
	}
	checksums2 := make([]*models.ChecksumAttribute, 2)
	checksums2[0] = md5_2
	checksums2[1] = sha256_2
	genericFile2 := &models.GenericFile{
		Identifier: "ncsu.edu/ncsu.1840.16-2928/data/object.properties",
		ChecksumAttributes: checksums2,
	}

	genericFiles := make([]*models.GenericFile, 2)
	genericFiles[0] = genericFile1
	genericFiles[1] = genericFile2
	return genericFiles
}


func TestAnyFilesNeedSaving(t *testing.T) {
	tarResult := getTarResult()
	if tarResult.AnyFilesNeedSaving() == false {
		t.Errorf("AnyFilesNeedSaving should have returned true.")
	}
	for i := range tarResult.LocalFiles {
		tarResult.LocalFiles[i].NeedsSave = false
	}
	if tarResult.AnyFilesNeedSaving() == true {
		t.Errorf("AnyFilesNeedSaving should have returned false.")
	}
}

func TestFilePaths(t *testing.T) {
	tarResult := getTarResult()
	filepaths := tarResult.FilePaths()
	if len(filepaths) == 0 {
		t.Error("TarResult.FilePaths returned no file paths")
		return
	}
	for i, path := range filepaths {
		if path != testdata.ExpectedPaths[i] {
			t.Errorf("Expected filepath '%s', got '%s'", testdata.ExpectedPaths[i], path)
		}
	}
}

func TestGetFileByPath(t *testing.T) {
	tarResult := getTarResult()
	file := tarResult.GetFileByPath("data/ORIGINAL/1")
	if file == nil {
		t.Errorf("GetFileByPath() did not return expected file")
	}
	if file.Path != "data/ORIGINAL/1" {
		t.Errorf("GetFileByPath() returned the wrong file")
	}
	file2 := tarResult.GetFileByPath("file/does/not/exist")
	if file2 != nil {
		t.Errorf("GetFileByPath() returned a file when it shouldn't have")
	}
}

func TestAnyFilesCopiedToPreservation(t *testing.T) {
	tarResult := getTarResult()
	if tarResult.AnyFilesCopiedToPreservation() == false {
		t.Error("AnyFilesCopiedToPreservation should have returned true")
	}
	tarResult.LocalFiles[0].StorageURL = ""
	if tarResult.AnyFilesCopiedToPreservation() == false {
		t.Error("AnyFilesCopiedToPreservation should have returned true")
	}
	for i := range tarResult.LocalFiles {
		tarResult.LocalFiles[i].StorageURL = ""
	}
	if tarResult.AnyFilesCopiedToPreservation() == true {
		t.Error("AnyFilesCopiedToPreservation should have returned false")
	}
}

func TestAllFilesCopiedToPreservation(t *testing.T) {
	tarResult := getTarResult()
	if tarResult.AllFilesCopiedToPreservation() == false {
		t.Error("AllFilesCopiedToPreservation should have returned true")
	}
	tarResult.LocalFiles[0].StorageURL = ""
	if tarResult.AllFilesCopiedToPreservation() == true {
		t.Error("AllFilesCopiedToPreservation should have returned false")
	}
}

func TestMergeExistingFiles(t *testing.T) {
	tarResult := getTarResult()
	genericFiles := buildGenericFiles()
	tarResult.MergeExistingFiles(genericFiles)

	// Existing and changed.
	// File "ncsu.edu/ncsu.1840.16-2928/data/metadata.xml"
	file := tarResult.LocalFiles[0]
	if file.ExistingFile == false {
		t.Errorf("File should have been marked as an existing file")
	}
	if file.NeedsSave == false {
		t.Errorf("File should have been marked as needing to be saved")
	}

	// Existing but unchanged.
	// File "ncsu.edu/ncsu.1840.16-2928/data/object.properties"
	file = tarResult.LocalFiles[1]
	if file.ExistingFile == false {
		t.Errorf("File should have been marked as an existing file")
	}
	if file.NeedsSave == true {
		t.Errorf("File should have been marked as NOT needing to be saved")
	}

	// New file "data/ORIGINAL/1"
	file = tarResult.LocalFiles[2]
	if file.ExistingFile == true {
		t.Errorf("File NOT should have been marked as an existing file")
	}
	if file.NeedsSave == false {
		t.Errorf("File should have been marked as needing to be saved")
	}

	// New file "data/ORIGINAL/1-metadata.xml"
	file = tarResult.LocalFiles[3]
	if file.ExistingFile == true {
		t.Errorf("File NOT should have been marked as an existing file")
	}
	if file.NeedsSave == false {
		t.Errorf("File should have been marked as needing to be saved")
	}

}
