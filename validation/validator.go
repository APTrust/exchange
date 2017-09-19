package validation

import (
	"bufio"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/storage"
	"github.com/satori/go.uuid"
	"hash"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

const VALIDATION_DB_SUFFIX = ".valdb"

// Validator validates a BagIt bag using a BagValidationConfig
// object, which describes the bag's requirements.
type Validator struct {
	PathToBag                  string
	BagValidationConfig        *BagValidationConfig
	PreserveExtendedAttributes bool
	ObjIdentifier              string
	summary                    *models.WorkSummary
	intelObj                   *models.IntellectualObject
	tagFilesToParse            []string
	manifests                  []string
	tagManifests               []string
	requiredFiles              []string
	forbiddenFiles             []string
	calculateMd5               bool
	calculateSha256            bool

	// Note that we can have only one open reference to the BoltDB
	// at a time. If some other piece of code has this DB open,
	// the validator will not be able to open it. If the validator
	// has it open, others will not be able to open it.
	db *storage.BoltDB
}

// NewValidator creates a new Validator. Param pathToBag
// should be an absolute path to either the tarred bag (.tar file)
// or to the untarred bag (a directory). Param bagValidationConfig
// defines what we need to validate, in addition to the checksums in the
// manifests. If param preserveExtendedAttributes is true, the validator
// will preserve special data attributes used by the APTrust ingest
// process, AND will leave the .valdb validation database in place after
// it completes its work. If false, it will keep just enough data to validate
// file lists and checksums, and will delete the .valdb database when
// it's finished.
func NewValidator(pathToBag string, bagValidationConfig *BagValidationConfig, preserveExtendedAttributes bool) (*Validator, error) {
	err := validateParams(pathToBag, bagValidationConfig)
	if err != nil {
		return nil, err
	}
	calculateMd5 := util.StringListContains(bagValidationConfig.FixityAlgorithms, constants.AlgMd5)
	calculateSha256 := util.StringListContains(bagValidationConfig.FixityAlgorithms, constants.AlgSha256)
	tagFilesToParse := make([]string, 0)
	for pathToFile, filespec := range bagValidationConfig.FileSpecs {
		if filespec.ParseAsTagFile {
			tagFilesToParse = append(tagFilesToParse, pathToFile)
		}
	}
	validator := &Validator{
		PathToBag:                  pathToBag,
		BagValidationConfig:        bagValidationConfig,
		PreserveExtendedAttributes: preserveExtendedAttributes,
		summary:                    models.NewWorkSummary(),
		ObjIdentifier:              util.CleanBagName(path.Base(pathToBag)),
		manifests:                  make([]string, 0),
		tagManifests:               make([]string, 0),
		tagFilesToParse:            tagFilesToParse,
		requiredFiles:              make([]string, 0),
		forbiddenFiles:             make([]string, 0),
		calculateMd5:               calculateMd5,
		calculateSha256:            calculateSha256,
	}
	return validator, nil
}

// validateParams returns an error if there's a problem with the parameters
// pathToBag or bagValidationConfig.
func validateParams(pathToBag string, bagValidationConfig *BagValidationConfig) error {
	if !fileutil.FileExists(pathToBag) {
		return fmt.Errorf("Bag does not exist at %s", pathToBag)
	}
	if bagValidationConfig == nil {
		return fmt.Errorf("Param bagValidationConfig cannot be nil")
	}
	configErrors := bagValidationConfig.ValidateConfig()
	if len(configErrors) > 0 {
		errString := "BagValidationConfig has the following errors:"
		for _, e := range configErrors {
			errString += fmt.Sprintf("\n%s", e.Error())
		}
		return fmt.Errorf(errString)
	}
	err := bagValidationConfig.CompileFileNameRegex()
	if err != nil {
		return fmt.Errorf("Error in BagValidationConfig: %v", err)
	}
	return nil
}

// DBName returns the name of the BoltDB file where the validator keeps
// track of validation data.
func (validator *Validator) DBName() string {
	bagPath := validator.PathToBag
	if strings.HasSuffix(bagPath, ".tar") {
		bagPath = bagPath[0 : len(bagPath)-4]
	}
	if strings.HasSuffix(bagPath, string(os.PathSeparator)) {
		bagPath = bagPath[0 : len(bagPath)-1]
	}
	return fmt.Sprintf("%s%s", bagPath, VALIDATION_DB_SUFFIX)
}

// getIterator returns either a tar file iterator or a filesystem
// iterator, depending on whether we're reading a tarred bag or
// an untarred one.
func (validator *Validator) getIterator() (fileutil.ReadIterator, error) {
	if strings.HasSuffix(validator.PathToBag, ".tar") {
		return fileutil.NewTarFileIterator(validator.PathToBag)
	}
	return fileutil.NewFileSystemIterator(validator.PathToBag)
}

// Validate reads and validates the bag, and returns a ValidationResult with
// the IntellectualObject and any errors encountered during validation.
func (validator *Validator) Validate() (*models.WorkSummary, error) {
	db, err := storage.NewBoltDB(validator.DBName())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	validator.db = db
	validator.summary.Start()
	validator.summary.Attempted = true
	validator.summary.AttemptNumber += 1
	validator.readBag()
	validator.verifyManifestPresent()
	validator.verifyTopLevelFolder()
	validator.verifyFileSpecs()
	validator.verifyTagSpecs()
	validator.verifyGenericFiles()
	validator.summary.Finish()
	return validator.summary, nil
}

// readBag reads through the contents of the bag and creates a list of
// GenericFiles. This function creates a lightweight record of the
// IntellectualObject in the db, and a for each file in the bag
// (payload files, manifests, and everything else).
func (validator *Validator) readBag() {
	// Call this for the side-effect of initializing the IntellectualObject
	// if it doesn't already exist.
	obj, err := validator.getIntellectualObject()
	if err != nil {
		validator.summary.AddError("Could not init object: %v", err)
		return
	}
	validator.intelObj = obj

	// Add all files in the bag to the GenericFiles list
	validator.addFiles()

	// Parse the files that can be parsed (manifests & plaintext tag files)
	validator.parseFiles()

	err = validator.db.Save(obj.Identifier, obj)
	if err != nil {
		validator.summary.AddError("Could not save intelObj metadata: %v", err)
	}
}

// getIntellectualObject returns a lightweight representation of the
// IntellectualObject that this bag represents. The IntellectualObject
// will not include PremisEvents or GenericFiles. GenericFiles are
// stored separately in the db.
func (validator *Validator) getIntellectualObject() (*models.IntellectualObject, error) {
	if validator.intelObj != nil {
		return validator.intelObj, nil
	}
	obj, err := validator.db.GetIntellectualObject(validator.ObjIdentifier)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return validator.initIntellectualObject()
	}
	return obj, err
}

// initIntellectualObject creates a barebones IntellectualObject.
func (validator *Validator) initIntellectualObject() (*models.IntellectualObject, error) {
	obj := models.NewIntellectualObject()
	obj.Identifier = validator.ObjIdentifier
	if strings.HasSuffix(validator.PathToBag, ".tar") {
		obj.IngestTarFilePath = validator.PathToBag
	} else {
		obj.IngestUntarredPath = validator.PathToBag
	}
	err := validator.db.Save(obj.Identifier, obj)
	return obj, err
}

// addFiles adds a record for each file to our validation database.
func (validator *Validator) addFiles() {
	iterator, err := validator.getIterator()
	if err != nil {
		validator.summary.AddError("Error getting file iterator: %v", err)
		return
	}
	for {
		err := validator.addFile(iterator)
		if err != nil && (err == io.EOF || err.Error() == "EOF") {
			break // readIterator hit the end of the list
		} else if err != nil {
			validator.summary.AddError("Error reading bag: %s", err.Error())
			validator.summary.ErrorIsFatal = true
			break // PT #146289839: Stop on error, or memory usage explodes.
		}
	}
	validator.intelObj.IngestTopLevelDirNames = iterator.GetTopLevelDirNames()
	validator.intelObj.IngestManifests = validator.manifests
	validator.intelObj.IngestTagManifests = validator.tagManifests
}

// addFile adds a record for a single file to our validation database.
func (validator *Validator) addFile(readIterator fileutil.ReadIterator) error {
	reader, fileSummary, err := readIterator.Next()
	if err != nil {
		return err
	}
	if !fileSummary.IsRegularFile {
		return nil
	}
	gf := models.NewGenericFile()
	gf.Identifier = fmt.Sprintf("%s/%s", validator.ObjIdentifier, fileSummary.RelPath)

	// Unfortunately, we need this to compute gf.OriginalPath()
	gf.IntellectualObjectIdentifier = validator.ObjIdentifier

	// Figure out whether this is a manifest, payload file, etc.
	// This is not the same as setting the file's mime type.
	validator.setFileType(gf, fileSummary)

	// The following info is used by the APTrust ingest process,
	// but is not relevant to anyone doing validation outside
	// the APTrust organization.
	if validator.PreserveExtendedAttributes {
		_uuid := uuid.NewV4()
		gf.Size = fileSummary.Size
		gf.FileModified = fileSummary.ModTime
		gf.IngestLocalPath = fileSummary.AbsPath // will be empty if bag is tarred
		gf.IngestNeedsSave = true                // default until proven otherwise
		gf.IngestUUID = _uuid.String()
		gf.IngestUUIDGeneratedAt = time.Now().UTC()
		gf.IngestFileUid = fileSummary.Uid
		gf.IngestFileGid = fileSummary.Gid
		validator.setMimeType(gf)
	}

	// Keep track of which required/forbidden files we encounter.
	fileSpec, isInSpec := validator.BagValidationConfig.FileSpecs[gf.OriginalPath()]
	if isInSpec {
		if fileSpec.Presence == REQUIRED {
			validator.requiredFiles = append(validator.requiredFiles, gf.OriginalPath())
		} else if fileSpec.Presence == FORBIDDEN {
			validator.forbiddenFiles = append(validator.forbiddenFiles, gf.OriginalPath())
		}
	}

	// We calculate checksums in all contexts, because that's part of
	// basic bag validation. Even if checksum calculation fails (which
	// has not yet happened), we still want to keep a record of the
	// GenericFile in the validation DB for later reporting purposes.
	checksumError := validator.calculateChecksums(reader, gf)
	saveError := validator.db.Save(gf.Identifier, gf)
	if checksumError != nil {
		return checksumError
	}
	return saveError
}

// calculateChecksums calculates the checksums on the given GenericFile.
// Depending on the config options, we may calculate multiple checksums
// in a single pass. (One of the perks of golang's MultiWriter.)
func (validator *Validator) calculateChecksums(reader io.Reader, gf *models.GenericFile) error {
	hashes := make([]io.Writer, 0)
	var md5Hash hash.Hash
	var sha256Hash hash.Hash
	if validator.calculateMd5 {
		md5Hash = md5.New()
		hashes = append(hashes, md5Hash)
	}
	if validator.calculateSha256 {
		sha256Hash = sha256.New()
		hashes = append(hashes, sha256Hash)
	}
	if len(hashes) > 0 {
		multiWriter := io.MultiWriter(hashes...)
		io.Copy(multiWriter, reader)
		utcNow := time.Now().UTC()
		if md5Hash != nil {
			gf.IngestMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
			if validator.PreserveExtendedAttributes {
				gf.IngestMd5GeneratedAt = utcNow
			}
		}
		if sha256Hash != nil {
			gf.IngestSha256 = fmt.Sprintf("%x", sha256Hash.Sum(nil))
			if validator.PreserveExtendedAttributes {
				gf.IngestSha256GeneratedAt = utcNow
			}
		}
	}
	return nil
}

// setFileType figures whether a file is a manifest, tag manifest,
// tag file or payload file. If the file is a manifest that we'll
// need to parse during the second phase of validation, this will
// add it to the list of Manifests or TagManifests.
func (validator *Validator) setFileType(gf *models.GenericFile, fileSummary *fileutil.FileSummary) {
	if strings.HasPrefix(fileSummary.RelPath, "tagmanifest-") {
		gf.IngestFileType = constants.TAG_MANIFEST
		gf.FileFormat = "text/plain"
		validator.tagManifests = append(validator.tagManifests, fileSummary.RelPath)
	} else if strings.HasPrefix(fileSummary.RelPath, "manifest-") {
		gf.IngestFileType = constants.PAYLOAD_MANIFEST
		gf.FileFormat = "text/plain"
		validator.manifests = append(validator.manifests, fileSummary.RelPath)
	} else if strings.HasPrefix(fileSummary.RelPath, "data/") {
		gf.IngestFileType = constants.PAYLOAD_FILE
	} else {
		gf.IngestFileType = constants.TAG_FILE
	}
}

// parseFiles parses files that the bagging config says to parse,
// like manifests and certain tag files.
func (validator *Validator) parseFiles() {
	// We have to get a new iterator here, because if we're
	// dealing with a TarFileIterator (which is likely), it's
	// forward-only. We can't rewind it.
	readIterator, err := validator.getIterator()
	if err != nil {
		validator.summary.AddError("Error getting file iterator: %v", err)
		return
	}
	for {
		// Don't use "defer reader.Close()" because the readers
		// won't be closed until we exit the enclosing funcion,
		// and the for loop may have opened 100k+ files by then.
		reader, fileSummary, err := readIterator.Next()

		// EOF means there are no more files to iterate through.
		if err == io.EOF {
			if reader != nil {
				reader.Close()
			}
			return
		}

		// Non EOF error means something went wrong.
		// Try to avoid out of memory exception, which
		// is either coming from too many errors or
		// one gigantic error.
		if err != nil {
			if reader != nil {
				reader.Close()
			}
			msg := ""
			if len(err.Error()) > 2000 {
				msg = err.Error()[0:2000]
			} else {
				msg = err.Error()
			}
			validator.summary.AddError(msg)
			if len(validator.summary.Errors) > 100 {
				if reader != nil {
					reader.Close()
				}
				return
			}
			continue
		}

		// If the iterator returns a directory, skip it.
		if fileSummary.IsDir {
			if reader != nil {
				reader.Close()
			}
			continue
		}

		// At this point, we should have an open reader
		// pointing to a legitimate file.
		gfIdentifier := fmt.Sprintf("%s/%s", validator.ObjIdentifier, fileSummary.RelPath)
		gf, err := validator.db.GetGenericFile(gfIdentifier)
		if err != nil {
			validator.summary.AddError("Error finding '%s' in validation db: %v", gfIdentifier, err)
			if reader != nil {
				reader.Close()
			}
			continue
		}
		if gf == nil {
			validator.summary.AddError("Cannot find '%s' in validation db", gfIdentifier)
			if reader != nil {
				reader.Close()
			}
			continue
		}
		validator.parseFile(reader, gf, fileSummary)
		if reader != nil {
			reader.Close()
		}
	}
}

// parseFile parses a file's contents if the file is a manifest,
// tag manifest, or parsable plain-text tag file. If the file
// doesn't match either of these cases, we skip it, and this is
// a no-op.
func (validator *Validator) parseFile(reader io.ReadCloser, gf *models.GenericFile, fileSummary *fileutil.FileSummary) {
	parseAsTagFile := util.StringListContains(validator.tagFilesToParse, fileSummary.RelPath)
	parseAsManifest := util.StringListContains(validator.manifests, fileSummary.RelPath) ||
		util.StringListContains(validator.tagManifests, fileSummary.RelPath)

	if parseAsTagFile {
		validator.parseTags(reader, fileSummary.RelPath)
	} else if parseAsManifest {
		// Get the checksums out of the manifest.
		validator.parseManifest(reader, fileSummary)
	}
}

// parseTags parses the tags in a bagit-format tag file. That's a plain-text
// file with names and values separated by a colon.
//
// TODO: Move this into a separate file and make it more generic.
func (validator *Validator) parseTags(reader io.Reader, relFilePath string) {
	obj, err := validator.getIntellectualObject()
	if err != nil {
		validator.summary.AddError("Error getting IntelObj from validation db: %v", err)
		return
	}
	if obj == nil {
		validator.summary.AddError("IntelObj '%s' is missing from validation db", validator.ObjIdentifier)
		return
	}
	re := regexp.MustCompile(`^(\S*\:)?(\s*.*)?$`)
	scanner := bufio.NewScanner(reader)
	var tag *models.Tag
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		if re.MatchString(line) {
			data := re.FindStringSubmatch(line)
			data[1] = strings.TrimSpace(data[1])
			data[1] = strings.Replace(data[1], ":", "", 1)
			if data[1] != "" {
				if tag != nil && tag.Label != "" {
					obj.IngestTags = append(obj.IngestTags, tag)
				}
				tag = models.NewTag(relFilePath, data[1], strings.TrimSpace(data[2]))
				validator.setIntelObjTagValue(obj, tag)
				continue
			}
			value := strings.TrimSpace(data[2])
			if tag != nil {
				tag.Value = strings.Join([]string{tag.Value, value}, " ")
				validator.setIntelObjTagValue(obj, tag)
			}
		} else {
			validator.summary.AddError("Unable to parse tag data from line: '%s'", line)
		}
	}
	if tag != nil && tag.Label != "" {
		obj.IngestTags = append(obj.IngestTags, tag)
	}
	if scanner.Err() != nil {
		validator.summary.AddError("Error reading tag file '%s': %v",
			relFilePath, scanner.Err().Error())
	}
	err = validator.db.Save(validator.ObjIdentifier, obj)
	if err != nil {
		validator.summary.AddError("Could not save IntelObj after parsing tags: %v", err)
	}
}

// Copy certain values from the aptrust-info.txt file into
// properties of the IntellectualObject. Although the
// institution name generally appears in the tag "Source-Organization",
// we don't want to set that here, because our ingest code needs
// the institution's identifier (domain name), not it's actual
// name. "Source-Organization" usually has something like
// "University of Virginia". We want "virginia.edu".
func (validator *Validator) setIntelObjTagValue(obj *models.IntellectualObject, tag *models.Tag) {
	if tag.SourceFile == "aptrust-info.txt" {
		label := strings.ToLower(tag.Label)
		switch label {
		case "title":
			obj.Title = tag.Value
		case "access":
			obj.Access = tag.Value
		}
	} else if tag.SourceFile == "bag-info.txt" {
		label := strings.ToLower(tag.Label)
		switch label {
		case "internal-sender-description":
			obj.Description = tag.Value
		case "internal-sender-identifier":
			obj.AltIdentifier = tag.Value
		}
	}
}

// Parse the checksums in a manifest.
//
// TODO: Move this into a separate file and make it more generic.
func (validator *Validator) parseManifest(reader io.Reader, fileSummary *fileutil.FileSummary) {
	alg := ""
	if strings.Contains(fileSummary.RelPath, constants.AlgSha256) {
		alg = constants.AlgSha256
	} else if strings.Contains(fileSummary.RelPath, constants.AlgMd5) {
		alg = constants.AlgMd5
	} else {
		fmt.Fprintln(os.Stderr, "Not verifying checksums in", fileSummary.RelPath,
			"- unsupported algorithm. Will still verify any md5 or sha256 checksums.")
		return
	}
	re := regexp.MustCompile(`^(\S*)\s*(.*)`)
	scanner := bufio.NewScanner(reader)
	lineNum := 1
	for scanner.Scan() {
		updateGenericFile := false
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		if re.MatchString(line) {
			data := re.FindStringSubmatch(line)
			digest := data[1]
			filePath := data[2]

			gfIdentifier := fmt.Sprintf("%s/%s", validator.ObjIdentifier, filePath)
			genericFile, err := validator.db.GetGenericFile(gfIdentifier)
			if err != nil {
				validator.summary.AddError("Error finding generic file '%s' in db: %v", gfIdentifier)
			}
			if genericFile == nil {
				validator.summary.AddError(
					"File '%s' in manifest '%s' is missing from bag",
					filePath, fileSummary.RelPath)
				continue
			}

			// If we got a digest from this line of the manifest,
			// set it on the GenericFile and save the record back
			// to the database.
			if alg == constants.AlgMd5 {
				genericFile.IngestManifestMd5 = digest
				updateGenericFile = true
			} else if alg == constants.AlgSha256 {
				genericFile.IngestManifestSha256 = digest
				updateGenericFile = true
			}
			if updateGenericFile {
				err = validator.db.Save(gfIdentifier, genericFile)
				if err != nil {
					validator.summary.AddError("Error saving generic file '%s' to db: %v", gfIdentifier, err)
				}
			}
		} else {
			validator.summary.AddError(fmt.Sprintf(
				"Unable to parse data from line %d of manifest %s: %s",
				lineNum, fileSummary.RelPath, line))
		}
		lineNum += 1
	}
}

// verifyManifestPresent checks to see if at least one payload manifest
// is present in the bag. If not, it adds an error message to the
// WorkSummary.
func (validator *Validator) verifyManifestPresent() {
	if len(validator.manifests) == 0 {
		validator.summary.AddError("Bag contains no payload manifest.")
	}
}

// verifyTopLevelFolder ensures the top-level folder inside a tar file
// has the same name as the bag. There should be exactly one top-level
// folder whose name is the same as the bag. Anything else is an error.
func (validator *Validator) verifyTopLevelFolder() {
	obj, err := validator.getIntellectualObject()
	if err != nil {
		validator.summary.AddError("Can't get object: %v", err)
		return
	}
	if obj.IngestTarFilePath == "" {
		return
	}
	re := regexp.MustCompile("\\.tar$")
	baseName := path.Base(obj.IngestTarFilePath)
	expectedDirName := re.ReplaceAllString(baseName, "")
	dirNames := obj.IngestTopLevelDirNames
	if dirNames != nil {
		for _, dirName := range dirNames {
			if dirName != expectedDirName {
				validator.summary.AddError(
					"Tarred bag should untar to directory '%s', not '%s'",
					expectedDirName, dirName)
			}
		}
	}
}

// verifyFileSpecs ensures required files are present and forbidden files
// are not. This adds an error to the WorkSummary for any violations.
func (validator *Validator) verifyFileSpecs() {
	for gfPath, fileSpec := range validator.BagValidationConfig.FileSpecs {
		if fileSpec.Presence == REQUIRED && !util.StringListContains(validator.requiredFiles, gfPath) {
			validator.summary.AddError("Required file '%s' is missing.", gfPath)
		} else if fileSpec.Presence == FORBIDDEN && util.StringListContains(validator.forbiddenFiles, gfPath) {
			validator.summary.AddError("Bag contains forbidden file '%s'.", gfPath)
		}
	}
}

// verifyTagSpecs ensures required tags are present and values are allowed.
func (validator *Validator) verifyTagSpecs() {
	obj, err := validator.getIntellectualObject()
	if err != nil {
		validator.summary.AddError("Cannot get object metadata from db: %v")
		return
	}
	for tagName, tagSpec := range validator.BagValidationConfig.TagSpecs {
		tags := obj.FindTag(tagName)
		if tagSpec.Presence == FORBIDDEN {
			validator.summary.AddError("Forbidden tag '%s' found in file '%s'.",
				tagName, tags[0].SourceFile)
			continue
		}
		if tagSpec.Presence == REQUIRED {
			validator.checkRequiredTag(tagName, tags, tagSpec)
		}
		if tags != nil && tagSpec.AllowedValues != nil && len(tagSpec.AllowedValues) > 0 {
			validator.checkAllowedTagValue(tagName, tags, tagSpec)
		}
	}
}

// checkRequiredTag ensures that a required tag is present.
// It adds and error to the WorkSummary if not.
func (validator *Validator) checkRequiredTag(tagName string, tags []*models.Tag, tagSpec TagSpec) {
	if tags == nil {
		validator.summary.AddError("Required tag '%s' is missing.", tagName)
		return
	}
	if !tagSpec.EmptyOK {
		tagHasValue := false
		for _, tag := range tags {
			if tag.Value != "" {
				tagHasValue = true
				break
			}
		}
		if !tagHasValue {
			validator.summary.AddError("Value for tag '%s' is missing.", tagName)
		}
	}
}

// checkAllowedTagValue ensures that the value of a tag is one of a
// set of values enumerated in the BagValidationConfig. It adds an
// error to the WorkSummary if not.
func (validator *Validator) checkAllowedTagValue(tagName string, tags []*models.Tag, tagSpec TagSpec) {
	valueOk := false
	lastValue := ""
	for _, value := range tagSpec.AllowedValues {
		for _, tag := range tags {
			lcValue := strings.TrimSpace(strings.ToLower(value))
			tagValue := strings.TrimSpace(strings.ToLower(tag.Value))
			lastValue = tagValue
			if lcValue == tagValue {
				valueOk = true
			}
		}
	}
	if !valueOk {
		validator.summary.AddError("Tag '%s' has illegal value '%s'.", tagName, lastValue)
	}
}

// verifyGenericFiles verifies a number of attributes related to generic files,
// including their checksums, presence in payload manifests, and whether they
// follow specified naming restrictions.
func (validator *Validator) verifyGenericFiles() {
	detail := validator.fileValidationDetail()
	gfIdentifiers := validator.db.FileIdentifiers()
	for _, gfIdentifier := range gfIdentifiers {
		gf, err := validator.db.GetGenericFile(gfIdentifier)

		// Md5 digests
		if gf.IngestManifestMd5 != "" && gf.IngestManifestMd5 != gf.IngestMd5 {
			validator.summary.AddError(
				"Bad md5 digest for '%s': manifest says '%s', file digest is '%s'",
				gf.OriginalPath(), gf.IngestManifestMd5, gf.IngestMd5)
		} else {
			gf.IngestMd5VerifiedAt = time.Now().UTC()
		}
		// Sha256 digests
		if gf.IngestManifestSha256 != "" && gf.IngestManifestSha256 != gf.IngestSha256 {
			validator.summary.AddError(
				"Bad sha256 digest for '%s': manifest says '%s', file digest is '%s'",
				gf.OriginalPath(), gf.IngestManifestSha256, gf.IngestSha256)
		} else {
			gf.IngestSha256VerifiedAt = time.Now().UTC()
		}
		// No manifest entry?
		if gf.IngestFileType == constants.PAYLOAD_FILE &&
			gf.IngestManifestMd5 == "" && gf.IngestManifestSha256 == "" {
			validator.summary.AddError(
				"File '%s' does not appear in any payload manifest (md5 or sha256)",
				gf.OriginalPath())
		}
		// Make sure name is valid
		if validator.BagValidationConfig.FileNameRegex != nil {
			for _, pathComponent := range strings.Split(gf.OriginalPath(), "/") {
				if !validator.BagValidationConfig.FileNameRegex.MatchString(pathComponent) {
					validator.summary.AddError(
						"Filename '%s' is not valid according to %s",
						gf.OriginalPath(), detail)
				}
			}
		}
		err = validator.db.Save(gf.Identifier, gf)
		if err != nil {
			validator.summary.AddError("Cannot save GenericFile %s to db after comparing checksums",
				gf.Identifier)
		}
	}
}

// fileValidationDetail returns a specific description of the file name
// validation rules in effect.
func (validator *Validator) fileValidationDetail() string {
	detail := "validation pattern " + validator.BagValidationConfig.FileNamePattern
	if strings.ToUpper(validator.BagValidationConfig.FileNamePattern) == "APTRUST" {
		detail = "APTrust validation rules"
	} else if strings.ToUpper(validator.BagValidationConfig.FileNamePattern) == "POSIX" {
		detail = "POSIX validation rules"
	}
	return detail
}

// setMimeType sets the FileFormat attribute of a GenericFile, based
// on the file's extension.
func (validator *Validator) setMimeType(gf *models.GenericFile) {
	mimeType := "application/binary"
	ext := path.Ext(gf.Identifier)
	if len(ext) > 1 {
		ext = ext[1:] // get rid of leading dot
		if t, ok := fileutil.MimeTypes[ext]; ok {
			mimeType = t
		}
	}
	gf.FileFormat = mimeType
}
