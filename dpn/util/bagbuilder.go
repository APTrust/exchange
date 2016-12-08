package util

import (
	"fmt"
	"github.com/APTrust/bagins"
	"github.com/APTrust/exchange/dpn"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/satori/go.uuid"
	"os"
	"path/filepath"
	"time"
)

// BagBuilder builds a DPN bag from an APTrust intellectual object.
type BagBuilder struct {
	// LocalPath is the full, absolute path the the untarred bag
	// the builder will create. It will end with the bag's UUID,
	// so it should look something like this:
	// /mnt/dpn/bags/00000000-0000-0000-0000-000000000000.
	LocalPath string

	// IntellectualObject is the APTrust IntellectualObject that
	// we'll be repackaging as a DPN bag.
	IntellectualObject *apt_models.IntellectualObject

	// DefaultMetadata is some metadata that goes into EVERY DPN
	// bag we create. This includes our name and address in the
	// DPN data section that describes who packaged the bag.
	// DefaultMetadata should be loaded from a JSON file using
	// the dpn.LoadConfig() function.
	DefaultMetadata apt_models.DefaultMetadata

	// UUID is the DPN identifier for this bag. This has nothing to
	// do with any APTrust UUID. It's generated in the constructor.
	UUID string

	// ErrorMessage describes what when wrong while trying to
	// package this bag. If it's an empty string, packaging
	// succeeded.
	ErrorMessage string

	// What type of bag is this? Data, rights or interpretive?
	BagType string

	// The underlying bag object.
	Bag *bagins.Bag

	// Timestamp describing when the bag was assembled.
	bagtime time.Time
}

// NewBagBuilder returns a new BagBuilder.
// Param localPath is the path to which the bag builder should write the
// DPN bag. Param obj is an IntellectualObject containing metadata
// about the APTrust bag that we'll be repackaging. Param defaultMetadata
// contains default metadata, such as the BagIt version, ingest node name,
// etc.
//
// The BagBuilder just creates the skeleton of a valid DPN bag, with
// the required files. After you create this, call the following for
// each file you want to put in the bag's data directory:
//
//   err := builder.Bag.AddFile("/abs/path/to/source.txt", "rel/path/to/dest.txt")
//
// That will copy the file at "/abs/path/to/source.txt" into the data
// directory at "rel/path/to/dest.txt", so its full relative path inside
// the bag would be "data/rel/path/to/dest.txt"
//
// You can also add non-payload files outside the data directory. That
// usually means adding custom tag files to custom tag directories.
//
//   err := builder.Bag.AddCustomTagfile("/abs/path/to/source.txt", "rel/path/to/dest.txt", true)
//
// That adds "/abs/path/to/source.txt" into "rel/path/to/dest.txt" inside
// the bag, but notice it's not in the data directory. The final param
// to AddCustomTagfile indicates whether you want to put the tag file's
// checksum in the tag manifest.
//
// You should not have to add any of the DPN standard tag files or
// manifests. BagBuilder does that for you.
//
// When you're done adding files to the bag, call this to write it all
// out to disk:
//
//  errors := builder.Bag.Save()
func NewBagBuilder(localPath string, obj *apt_models.IntellectualObject, defaultMetadata apt_models.DefaultMetadata) (*BagBuilder, error) {
	uuid := uuid.NewV4().String()
	filePath, err := filepath.Abs(localPath)
	if err != nil {
		return nil, err
	}

	// Do this, or bagins.NewBag fails
	err = os.MkdirAll(localPath, 0755)
	if err != nil {
		return nil, err
	}

	originalBagName := obj.BagName
	bag, err := bagins.NewBag(filePath, originalBagName, []string{"sha256"}, true)
	if err != nil {
		return nil, err
	}
	builder := &BagBuilder{
		LocalPath:          filepath.Join(filePath, originalBagName),
		IntellectualObject: obj,
		DefaultMetadata:    defaultMetadata,
		UUID:               uuid,
		BagType:            dpn.BAG_TYPE_DATA,
		Bag:                bag,
	}

	err = os.MkdirAll(filepath.Join(builder.LocalPath, "dpn-tags"), 0755)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(filepath.Join(builder.LocalPath, "data"), 0755)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(filepath.Join(builder.LocalPath, "aptrust-tags"), 0755)
	if err != nil {
		return nil, err
	}

	builder.buildAPTrustBagIt()
	builder.buildDPNBagIt()
	builder.buildDPNBagInfo()
	builder.buildDPNInfo()

	return builder, nil
}

// BagTime returns the datetime the bag was created,
// in RFC3339 format (e.g. "2015-03-05T10:10:00Z")
func (builder *BagBuilder) BagTime() string {
	if builder.bagtime.IsZero() {
		builder.bagtime = time.Now().UTC()
	}
	return builder.bagtime.Format(time.RFC3339)
}

func (builder *BagBuilder) buildDPNBagIt() {
	bagit, err := builder.AddTagFile("bagit.txt")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	bagit.Data.AddField(*bagins.NewTagField("BagIt-Version",
		builder.DefaultMetadata.BagItVersion))
	bagit.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding",
		builder.DefaultMetadata.BagItEncoding))
}

func (builder *BagBuilder) buildDPNBagInfo() {
	bagInfo, err := builder.AddTagFile("bag-info.txt")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	bagInfo.Data.AddField(*bagins.NewTagField("Source-Organization",
		builder.IntellectualObject.Institution))
	bagInfo.Data.AddField(*bagins.NewTagField("Organization-Address", ""))
	bagInfo.Data.AddField(*bagins.NewTagField("Contact-Name", ""))
	bagInfo.Data.AddField(*bagins.NewTagField("Contact-Phone", ""))
	bagInfo.Data.AddField(*bagins.NewTagField("Contact-Email", ""))
	bagInfo.Data.AddField(*bagins.NewTagField("Bagging-Date", builder.BagTime()))

	// TODO: How can we put the bag size in a file that's inside the bag?
	bagInfo.Data.AddField(*bagins.NewTagField("Bag-Size",
		fmt.Sprintf("%d", builder.IntellectualObject.TotalFileSize())))
	bagInfo.Data.AddField(*bagins.NewTagField("Bag-Group-Identifier", ""))
	bagInfo.Data.AddField(*bagins.NewTagField("Bag-Count", "1"))
}

func (builder *BagBuilder) buildDPNInfo() {
	tagFilePath := filepath.Join("dpn-tags", "dpn-info.txt")
	dpnInfo, err := builder.AddTagFile(tagFilePath)
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	dpnInfo.Data.AddField(*bagins.NewTagField("DPN-Object-ID",
		builder.UUID))
	dpnInfo.Data.AddField(*bagins.NewTagField("Local-ID",
		builder.IntellectualObject.Identifier))
	dpnInfo.Data.AddField(*bagins.NewTagField("Ingest-Node-Name",
		builder.DefaultMetadata.IngestNodeName))
	dpnInfo.Data.AddField(*bagins.NewTagField("Ingest-Node-Address",
		builder.DefaultMetadata.IngestNodeAddress))
	dpnInfo.Data.AddField(*bagins.NewTagField("Ingest-Node-Contact-Name",
		builder.DefaultMetadata.IngestNodeContactName))
	dpnInfo.Data.AddField(*bagins.NewTagField("Ingest-Node-Contact-Email",
		builder.DefaultMetadata.IngestNodeContactEmail))

	// TODO: Not sure how to fill in the next three items.
	// We have to wait until DPN versioning spec is written, then we
	// need to know how to let depositors specify whether to overwrite
	// bags or save new versions in DPN, then we need a way of knowing
	// which DPN object this is a new version of, and which version
	// it should be.
	dpnInfo.Data.AddField(*bagins.NewTagField("Version-Number", "1"))
	// Are we also using First-Version-Object-ID?
	// Check https://wiki.duraspace.org/display/DPN/BagIt+Specification
	// for updates.
	dpnInfo.Data.AddField(*bagins.NewTagField("First-Version-Object-ID",
		builder.UUID))
	dpnInfo.Data.AddField(*bagins.NewTagField("Interpretive-Object-ID", ""))
	dpnInfo.Data.AddField(*bagins.NewTagField("Rights-Object-ID", ""))

	// Bag Type
	dpnInfo.Data.AddField(*bagins.NewTagField("Bag-Type",
		builder.BagType))
}

func (builder *BagBuilder) buildAPTrustBagIt() {
	aptrustBagit, err := builder.AddTagFile("aptrust-tags/bagit.txt")
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	if err != nil {
		builder.ErrorMessage += fmt.Sprintf("[%s] ", err.Error())
		return
	}
	aptrustBagit.Data.AddField(*bagins.NewTagField("BagIt-Version",
		dpn.APTRUST_BAGIT_VERSION))
	aptrustBagit.Data.AddField(*bagins.NewTagField("Tag-File-Character-Encoding",
		dpn.APTRUST_BAGIT_ENCODING))
}

// AddTagFile creates a new tag file and adds it to the bag,
// one level up from the data directory. After you add the tag
// file, you can programmatically define its contents by calling
//
// if err := builder.AddTagfile("bag-info.txt"); err != nil {
//    return err
// }
// tagFile, err := builder.Bag.TagFile("bag-info.txt")
// if err != nil {
//    return err
// }
//
// tagFile.Data.AddField(*bagins.NewTagField("Source-Organization", "uva.edu"))
// tagFile.Data.AddField(*bagins.NewTagField("Bag-Count", "1"))
//
// If you want to copy an existing file into your bag as a tag file, use
// builder.Bag.AddCustomTagfile(absSourcePath, relDestPath string)
func (builder *BagBuilder) AddTagFile(tagFileName string) (*bagins.TagFile, error) {
	err := builder.Bag.AddTagfile(tagFileName)
	if err != nil {
		return nil, fmt.Errorf("Error adding tag file %s: %s", tagFileName, err.Error())
	}
	tagFile, err := builder.Bag.TagFile(tagFileName)
	if err != nil {
		return nil, fmt.Errorf("Error retrieving tag file %s: %s", tagFileName, err.Error())
	}
	return tagFile, nil
}
