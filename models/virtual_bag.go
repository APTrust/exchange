package models

import (
	"bufio"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/platform"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nu7hatch/gouuid"
	"hash"
	"io"
	"path"
	"regexp"
	"strings"
	"time"
)

// VirtualBag creates an IntellectualObject from a bag on disk.
// The IntellectualObject can then be validated by workers.BagValidator.
type VirtualBag struct {
	pathToBag        string
	calculateMd5     bool
	calculateSha256  bool
	tagFilesToParse  []string
	obj              *IntellectualObject
	summary          *WorkSummary
	readIterator     fileutil.ReadIterator
}

// NewVirtualBag creates a new virtual bag. Param pathToBag should
// be an absolute path to either a tar file or a directory containing
// an untarred bag. It pathToBag points to a tar file, the Read()
// function will read the bag without untarring it. Param tagFilesToParse
// should be a list of relative paths, pointing to tag files within the
// bag that should be parsed. For example, "aptrust-info.txt" or
// "dpn_tags/dpn-info.txt" Params calculateMd5 and calculateSha256
// indicate whether we should calculate md5 and/or sha256 checksums
// on the files in the bag.
func NewVirtualBag(pathToBag string, tagFilesToParse []string, calculateMd5, calculateSha256 bool) (*VirtualBag) {
	if tagFilesToParse == nil {
		tagFilesToParse = make([]string, 0)
	}
	return &VirtualBag{
		calculateMd5: calculateMd5,
		calculateSha256: calculateSha256,
		pathToBag: pathToBag,
		tagFilesToParse: tagFilesToParse,
	}
}

// Read() reads the bag and returns an IntellectualObject and a WorkSummary.
// The WorkSummary will include a list of errors, if there were any.
// The list of files contained in IntellectualObject.GenericFiles will include
// ALL files found in the bag, even some we may not want to save, such as
// those beginning with dots and dashes. If you don't want to preserve those
// files you can delete them from the IntellectualObject manually later.
func (vbag *VirtualBag) Read() (*IntellectualObject, *WorkSummary) {
	vbag.summary = NewWorkSummary()
	vbag.summary.Start()
	vbag.obj = NewIntellectualObject()
	vbag.obj.Identifier, _ = util.CleanBagName(path.Base(vbag.pathToBag))
	if strings.HasSuffix(vbag.pathToBag, ".tar") {
		vbag.obj.IngestTarFilePath = vbag.pathToBag
	} else {
		vbag.obj.IngestUntarredPath = vbag.pathToBag
	}

	// Compile a list of the bag's contents (GenericFiles),
	// and calculate checksums for everything in the bag.
	var err error
	if vbag.obj.IngestTarFilePath != "" {
		vbag.readIterator, err = fileutil.NewTarFileIterator(vbag.obj.IngestTarFilePath)
	} else {
		vbag.readIterator, err = fileutil.NewFileSystemIterator(vbag.obj.IngestUntarredPath)
	}
	if err != nil {
		vbag.summary.AddError("Could not read bag: %v", err)
	} else {
		vbag.addGenericFiles()
	}


	// Golang's tar file reader is forward-only, so we need to
	// open a new iterator to read through a handful of tag files,
	// manifests and tag manifests.
	vbag.readIterator = nil
	if vbag.obj.IngestTarFilePath != "" {
		vbag.readIterator, err = fileutil.NewTarFileIterator(vbag.obj.IngestTarFilePath)
	} else {
		vbag.readIterator, err = fileutil.NewFileSystemIterator(vbag.obj.IngestUntarredPath)
	}
	if err != nil {
		vbag.summary.AddError("Could not read bag: %v", err)
	} else {
		vbag.parseManifestsAndTagFiles()
	}
	vbag.summary.Finish()
	return vbag.obj, vbag.summary
}

// Add every file in the bag to the list of generic files.
func (vbag *VirtualBag) addGenericFiles() () {
	for {
		err := vbag.addGenericFile()
		if err == io.EOF {
			break
		} else if err != nil {
			vbag.summary.AddError(err.Error())
		}
	}
}

// Adds a single generic file to the bag.
func (vbag *VirtualBag) addGenericFile() (error) {
	reader, fileSummary, err := vbag.readIterator.Next()
	if err != nil {
		return err
	}
	if !fileSummary.IsRegularFile {
		return nil
	}
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Can't read from /dev/urandom!")
	}
	gf := NewGenericFile()
	gf.Identifier = fmt.Sprintf("%s/%s", vbag.obj.Identifier, fileSummary.RelPath)
	gf.IntellectualObjectIdentifier = vbag.obj.Identifier
	gf.Size = fileSummary.Size
	gf.FileModified = fileSummary.ModTime
	gf.IngestLocalPath = fileSummary.AbsPath // will be empty if bag is tarred
	gf.IngestUUID = uuid.String()
	gf.IngestUUIDGeneratedAt = time.Now().UTC()
	gf.IngestFileUid = fileSummary.Uid
	gf.IngestFileGid = fileSummary.Gid
	vbag.obj.GenericFiles = append(vbag.obj.GenericFiles, gf)
	vbag.setIngestFileType(gf, fileSummary)
	return vbag.calculateChecksums(reader, gf)
}

// Figure out what type of file this is.
func (vbag *VirtualBag) setIngestFileType(gf *GenericFile, fileSummary *fileutil.FileSummary) {
	if strings.HasPrefix(fileSummary.RelPath, "tagmanifest-") {
		gf.IngestFileType = constants.TAG_MANIFEST
		vbag.obj.IngestTagManifests = append(vbag.obj.IngestTagManifests, fileSummary.RelPath)
	} else if strings.HasPrefix(fileSummary.RelPath, "manifest-") {
		gf.IngestFileType = constants.PAYLOAD_MANIFEST
		vbag.obj.IngestManifests = append(vbag.obj.IngestManifests, fileSummary.RelPath)
	} else if strings.HasPrefix(fileSummary.RelPath, "data/") {
		gf.IngestFileType = constants.PAYLOAD_FILE
	} else {
		gf.IngestFileType = constants.TAG_FILE
	}
}

// Calculate the md5 and/or sha256 checksums on a file.
func (vbag *VirtualBag) calculateChecksums(reader io.Reader, gf *GenericFile) (error) {
	hashes := make([]io.Writer, 0)
	var md5Hash hash.Hash
	var sha256Hash hash.Hash
	if vbag.calculateMd5 {
		md5Hash = md5.New()
		hashes = append(hashes, md5Hash)
	}
	if vbag.calculateSha256 {
		sha256Hash = sha256.New()
		hashes = append(hashes, sha256Hash)
	}
	if len(hashes) > 0 {
		multiWriter := io.MultiWriter(hashes...)
		io.Copy(multiWriter, reader)
		utcNow := time.Now().UTC()
		if md5Hash != nil {
			gf.IngestMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
			gf.IngestMd5GeneratedAt = utcNow
		}
		if sha256Hash != nil {
			gf.IngestSha256 = fmt.Sprintf("%x", sha256Hash.Sum(nil))
			gf.IngestSha256GeneratedAt = utcNow
		}
	}
	// on err, defaults to application/binary
	buf := make([]byte, 1024)
	_, _ = reader.Read(buf)
	gf.FileFormat, _ = platform.GuessMimeTypeByBuffer(buf)
	return nil
}

func (vbag *VirtualBag) parseManifestsAndTagFiles() {
	for {
		reader, fileSummary, err := vbag.readIterator.Next()
		if reader != nil {
			defer reader.Close()
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			vbag.summary.AddError(err.Error())
			continue
		}
		if util.StringListContains(vbag.tagFilesToParse, fileSummary.RelPath) {
			vbag.parseTags(reader, fileSummary.RelPath)
		} else if util.StringListContains(vbag.obj.IngestManifests, fileSummary.RelPath) ||
			util.StringListContains(vbag.obj.IngestTagManifests, fileSummary.RelPath) {
			vbag.parseManifest(reader, fileSummary.RelPath)
		}
	}
}

// Parse the checksums in a manifest.
func (vbag *VirtualBag) parseManifest(reader io.Reader, relFilePath string) () {
	alg := constants.AlgMd5
	if strings.Contains(relFilePath, constants.AlgSha256) {
		alg = constants.AlgSha256
	}
	re := regexp.MustCompile(`^(\S*)\s*(.*)`)
	scanner := bufio.NewScanner(reader)
	lineNum := 1
	for scanner.Scan() {
		line := scanner.Text()
		if re.MatchString(line) {
			data := re.FindStringSubmatch(line)
			digest := data[1]
			filePath := data[2]
			genericFile := vbag.obj.FindGenericFile(filePath)
			if genericFile == nil {
				vbag.summary.AddError(
					"Manifest '%s' includes checksum for file '%s', which was not found in bag",
					relFilePath, filePath)
				vbag.obj.IngestMissingFiles = append(vbag.obj.IngestMissingFiles,
					NewMissingFile(relFilePath, lineNum, filePath, digest))
				continue
			}
			if alg == constants.AlgMd5 {
				genericFile.IngestManifestMd5 = digest
			} else if alg == constants.AlgSha256 {
				genericFile.IngestManifestSha256 = digest
			}
		} else {
			vbag.summary.AddError(fmt.Sprintf(
				"Unable to parse data from line %d of manifest %s: %s",
				lineNum, relFilePath, line))
		}
		lineNum += 1
	}
}

// Parse the tag fields in a file.
func (vbag *VirtualBag) parseTags(reader io.Reader, relFilePath string) () {
	re := regexp.MustCompile(`^(\S*\:)?(\s.*)?$`)
	scanner := bufio.NewScanner(reader)
	var tag *Tag
	for scanner.Scan() {
		line := scanner.Text()
		if re.MatchString(line) {
			data := re.FindStringSubmatch(line)
			data[1] = strings.Replace(data[1], ":", "", 1)
			if data[1] != "" {
				if tag != nil && tag.Label != "" {
					vbag.obj.IngestTags = append(vbag.obj.IngestTags, tag)
				}
				tag = NewTag(relFilePath, data[1], strings.Trim(data[2], " "))
				vbag.setIntelObjTagValue(tag)
				continue
			}
			value := strings.Trim(data[2], " ")
			tag.Value = strings.Join([]string{tag.Value, value}, " ")
			vbag.setIntelObjTagValue(tag)
		} else {
			vbag.summary.AddError("Unable to parse tag data from line: %s", line)
		}
	}
	if tag.Label != "" {
		vbag.obj.IngestTags = append(vbag.obj.IngestTags, tag)
	}
	if scanner.Err() != nil {
		vbag.summary.AddError("Error reading tag file '%s': %v",
			relFilePath, scanner.Err().Error())
	}
}

// Copy certain values from the aptrust-info.txt file into
// properties of the IntellectualObject.
func (vbag *VirtualBag) setIntelObjTagValue(tag *Tag) () {
	if tag.SourceFile == "aptrust-info.txt" {
		label := strings.ToLower(tag.Label)
		switch label {
		case "title": vbag.obj.Title = tag.Value
		case "access": vbag.obj.Access = tag.Value
		}
	} else if tag.SourceFile == "bag-info.txt" {
		label := strings.ToLower(tag.Label)
		switch label {
		case "source-organization": vbag.obj.Institution = tag.Value
		case "internal-sender-description": vbag.obj.Description = tag.Value
		case "internal-sender-identifier": vbag.obj.AltIdentifier = tag.Value
		}
	}

}
