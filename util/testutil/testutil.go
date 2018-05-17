package testutil

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/icrowley/fake"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Bloomsday
var TEST_TIMESTAMP time.Time = time.Date(2016, 6, 16, 10, 24, 16, 0, time.UTC)

var INTEGRATION_GOOD_BAGS = []string{
	"aptrust.integration.test/example.edu.tagsample_good.tar",
	"aptrust.integration.test/example.edu.sample_ds_store_and_empty.tar",
	"aptrust.integration.test/ncsu.1840.16-10.tar",
	"aptrust.integration.test/ncsu.1840.16-1004.tar",
	"aptrust.integration.test/ncsu.1840.16-1005.tar",
	"aptrust.integration.test/ncsu.1840.16-1013.tar",
	"aptrust.integration.test/ncsu.1840.16-1028.tar",
	"aptrust.integration.test/ncsu.1840.16-2928.tar",
	"aptrust.integration.test/virginia.edu.uva-lib_2141114.tar",
	"aptrust.integration.test/virginia.edu.uva-lib_2274642.tar",
	"aptrust.integration.test/virginia.edu.uva-lib_2274765.tar",
	"aptrust.integration.test/virginia.edu.uva-lib_2278801.tar",
}

var INTEGRATION_GLACIER_BAGS = []string{
	"aptrust.integration.test/example.edu.sample_glacier_oh.tar",
	"aptrust.integration.test/example.edu.sample_glacier_or.tar",
	"aptrust.integration.test/example.edu.sample_glacier_va.tar",
}

var INTEGRATION_BAD_BAGS = []string{
	"aptrust.integration.test/example.edu.tagsample_bad.tar",
	"aptrust.integration.test/s3_upload_test.tar",
	"aptrust.integration.test/TestBags.zip",
	"aptrust.integration.test/test.edu.bag2.tar",
	"aptrust.integration.test/test.edu.bag6.tar",
}

// Integration tests ingest a second, updated version of these bags.
const (
	UPDATED_BAG_IDENTIFIER = "test.edu/example.edu.tagsample_good"
	UPDATED_BAG_ETAG       = "ec520876f7c87e24f926a8efea390b26"

	UPDATED_GLACIER_BAG_IDENTIFIER = "test.edu/example.edu.sample_glacier_oh"
	UPDATED_GLACIER_BAG_ETAG       = "bf01126663915a4f5d135a37443b8349"
)

func ShouldRunIntegrationTests() bool {
	return os.Getenv("RUN_EXCHANGE_INTEGRATION") == "true"
}

func RunningInCI() bool {
	return os.Getenv("TRAVIS_BUILD_DIR") != ""
}

func MakeChecksum() *models.Checksum {
	return &models.Checksum{
		Id:            rand.Intn(50000) + 1,
		GenericFileId: rand.Intn(50000) + 1,
		Algorithm:     RandomAlgorithm(),
		DateTime:      RandomDateTime(),
		Digest:        fake.Sentence(),
	}
}

func MakeGenericFile(eventCount, checksumCount int, objIdentifier string) *models.GenericFile {
	if objIdentifier == "" {
		objIdentifier = RandomObjectIdentifier()
	}
	objIdParts := strings.Split(objIdentifier, "/")
	inst := objIdParts[0]
	objName := objIdParts[1]
	fileIdentifier := RandomFileIdentifier(objIdentifier)
	_uuid := uuid.NewV4()
	checksums := make([]*models.Checksum, checksumCount)
	events := make([]*models.PremisEvent, eventCount)
	for i := 0; i < checksumCount; i++ {
		checksums[i] = MakeChecksum()
	}
	for i := 0; i < eventCount; i++ {
		events[i] = MakePremisEvent()
	}
	return &models.GenericFile{
		Id:                           rand.Intn(50000) + 1,
		Identifier:                   fileIdentifier,
		IntellectualObjectId:         rand.Intn(50000) + 1,
		IntellectualObjectIdentifier: objIdentifier,
		FileFormat:                   RandomFileFormat(),
		URI:                          fmt.Sprintf("%s/%s.%s/%s", constants.S3UriPrefix, constants.ReceiveTestBucketPrefix, inst, objName),
		Size:                         int64(rand.Intn(5000000) + 1),
		State:                        "A",
		StorageOption:                constants.StorageStandard,
		FileCreated:                  RandomDateTime(),
		FileModified:                 RandomDateTime(),
		CreatedAt:                    RandomDateTime(),
		UpdatedAt:                    RandomDateTime(),
		Checksums:                    checksums,
		PremisEvents:                 events,
		IngestLocalPath:              fmt.Sprintf("/mnt/aptrust/data/%s", fileIdentifier),
		IngestMd5:                    fake.CharactersN(32),
		IngestMd5GeneratedAt:         RandomDateTime(),
		IngestMd5VerifiedAt:          RandomDateTime(),
		IngestSha256:                 fake.CharactersN(64),
		IngestSha256GeneratedAt:      RandomDateTime(),
		IngestUUID:                   _uuid.String(),
		IngestUUIDGeneratedAt:        RandomDateTime(),
		IngestStorageURL:             fmt.Sprintf("https://storage.aws/%s", _uuid.String()),
		IngestStoredAt:               RandomDateTime(),
		IngestReplicationURL:         fmt.Sprintf("https://replication.aws/%s", _uuid.String()),
		IngestReplicatedAt:           RandomDateTime(),
		IngestPreviousVersionExists:  false,
		IngestNeedsSave:              true,
		IngestErrorMessage:           "",
	}
}

func MakeInstitution() *models.Institution {
	return &models.Institution{
		Id:         rand.Intn(50000) + 1,
		Name:       fake.Product(),
		BriefName:  fake.Word(),
		Identifier: fake.DomainName(),
	}
}

func MakeIntellectualObject(fileCount, eventCount, checksumCount, tagCount int) *models.IntellectualObject {
	objIdentifier := RandomObjectIdentifier()
	objIdParts := strings.Split(objIdentifier, "/")
	inst := objIdParts[0]
	objName := objIdParts[1]
	files := make([]*models.GenericFile, fileCount)
	events := make([]*models.PremisEvent, eventCount)
	tags := make([]*models.Tag, tagCount)
	for i := 0; i < fileCount; i++ {
		files[i] = MakeGenericFile(eventCount, checksumCount, objIdentifier)
	}
	for i := 0; i < eventCount; i++ {
		events[i] = MakePremisEvent()
	}
	for i := 0; i < tagCount; i++ {
		tags[i] = MakeTag()
	}
	return &models.IntellectualObject{
		Id:                  rand.Intn(50000) + 1,
		Identifier:          objIdentifier,
		BagName:             objName,
		Institution:         inst,
		InstitutionId:       rand.Intn(50000) + 1,
		Title:               fake.Words(),
		Description:         fake.Sentence(),
		Access:              RandomAccess(),
		AltIdentifier:       fake.Word(),
		GenericFiles:        files,
		PremisEvents:        events,
		CreatedAt:           RandomDateTime(),
		UpdatedAt:           RandomDateTime(),
		DPNUUID:             uuid.NewV4().String(),
		ETag:                fake.Word(),
		State:               "A",
		StorageOption:       constants.StorageStandard,
		IngestS3Bucket:      fmt.Sprintf("%s.%s", constants.ReceiveTestBucketPrefix, inst),
		IngestS3Key:         fmt.Sprintf("%s.tar", objName),
		IngestTarFilePath:   fmt.Sprintf("/mnt/aptrust/data/%s/%s.tar", inst, objName),
		IngestUntarredPath:  fmt.Sprintf("/mnt/aptrust/data/%s/%s/", inst, objName),
		IngestRemoteMd5:     fake.Word(),
		IngestLocalMd5:      fake.Word(),
		IngestMd5Verified:   false,
		IngestMd5Verifiable: false,
		IngestFilesIgnored:  make([]string, 0),
		IngestTags:          tags,
		IngestErrorMessage:  "",
	}
}

func MakePremisEvent() *models.PremisEvent {
	_uuid := uuid.NewV4()
	return &models.PremisEvent{
		Id:                 rand.Intn(50000) + 1,
		Identifier:         _uuid.String(),
		EventType:          RandomEventType(),
		DateTime:           RandomDateTime(),
		Detail:             fake.Words(),
		Outcome:            fake.Word(),
		OutcomeDetail:      fake.Sentence(),
		Object:             fake.Words(),
		Agent:              fake.MaleFullName(),
		OutcomeInformation: fake.Sentence(),
	}
}

func MakeTag() *models.Tag {
	return &models.Tag{
		Label: fake.Word(),
		Value: fake.Sentence(),
	}
}

func MakeWorkItem() *models.WorkItem {
	return &models.WorkItem{
		Id:                    rand.Intn(50000) + 1,
		ObjectIdentifier:      RandomObjectIdentifier(),
		GenericFileIdentifier: "",
		Name:             fake.Word(),
		Bucket:           "aptrust.receiving.virginia.edu",
		ETag:             fake.Word(),
		BagDate:          RandomDateTime(),
		InstitutionId:    rand.Intn(50000) + 1,
		User:             fake.EmailAddress(),
		Date:             RandomDateTime(),
		Note:             fake.Sentence(),
		Action:           RandomAction(),
		Stage:            RandomStage(),
		Status:           RandomStatus(),
		Outcome:          fake.Sentence(),
		Retry:            true,
		Node:             fake.Word(),
		Pid:              rand.Intn(50000) + 1,
		NeedsAdminReview: false,
	}
}

func MakeWorkItemState() *models.WorkItemState {
	return &models.WorkItemState{
		Id:         rand.Intn(50000) + 1,
		WorkItemId: rand.Intn(50000) + 1,
		Action:     constants.ActionIngest,
		State:      `{"key1":"value1","key2":"value2"}`,
		CreatedAt:  RandomDateTime(),
		UpdatedAt:  time.Now().UTC(),
	}
}

func MakeDPNWorkItem() *models.DPNWorkItem {
	_uuid := uuid.NewV4()
	queuedAt := RandomDateTime()
	createdAt := RandomDateTime()
	note := fake.Sentence()
	return &models.DPNWorkItem{
		Id:          rand.Intn(50000) + 1,
		RemoteNode:  fake.Word(),
		Task:        RandomFromList(constants.DPNTaskTypes),
		Identifier:  _uuid.String(),
		QueuedAt:    &queuedAt,
		CompletedAt: nil,
		Note:        &note,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

func MakeWorkSummary() *models.WorkSummary {
	return &models.WorkSummary{
		Attempted:     true,
		AttemptNumber: 1,
		Errors:        make([]string, 0),
		StartedAt:     RandomDateTime(),
		FinishedAt:    time.Now().UTC(),
		Retry:         true,
	}
}

func MakeStoredFile() *models.StoredFile {
	now := time.Now().UTC()
	return &models.StoredFile{
		Id:           int64(rand.Intn(100000)),
		Key:          uuid.NewV4().String(),
		Bucket:       fake.Word(),
		Size:         int64(rand.Intn(900000000)),
		ContentType:  fake.Word(),
		Institution:  fake.Word(),
		BagName:      fake.Word(),
		PathInBag:    fake.Word(),
		Md5:          fake.Word(),
		Sha256:       fake.Word(),
		ETag:         fake.Word(),
		LastModified: now,
		LastSeenAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
		DeletedAt:    now,
	}
}

func MakePharosDPNBag() *models.PharosDPNBag {
	nodes := []string{"aptrust", "chron", "hathi", "sdr", "tdr"}
	return &models.PharosDPNBag{
		Id:               rand.Intn(50000) + 1,
		InstitutionId:    rand.Intn(20) + 1,
		ObjectIdentifier: RandomObjectIdentifier(),
		DPNIdentifier:    uuid.NewV4().String(),
		DPNSize:          uint64(rand.Intn(50000000) + 1),
		Node1:            RandomFromList(nodes),
		Node2:            RandomFromList(nodes),
		Node3:            RandomFromList(nodes),
		DPNCreatedAt:     RandomDateTime(),
		DPNUpdatedAt:     RandomDateTime(),
		CreatedAt:        RandomDateTime(),
		UpdatedAt:        RandomDateTime(),
	}
}

// MakeNsqMessage creates an NSQ Message with the specified body.
// For our purposes, param body should be an integer in string format,
// like "1234" or "999".
func MakeNsqMessage(body string) *nsq.Message {
	messageId := [nsq.MsgIDLength]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}
	return nsq.NewMessage(messageId, []byte(body))
}

func RandomDateTime() time.Time {
	t := time.Now().UTC()
	minutes := rand.Intn(500000) * -1
	return t.Add(time.Duration(minutes) * time.Minute)
}

func RandomAccess() string {
	access := []string{"consortia", "institution", "restricted"}
	return RandomFromList(access)
}

func RandomAlgorithm() string {
	algs := []string{"md5", "sha256", "sha512"}
	return RandomFromList(algs)
}

func RandomObjectIdentifier() string {
	return fmt.Sprintf("%s/%s", fake.DomainName(), fake.FemalePatronymic())
}

func RandomFileIdentifier(objectIdentifier string) string {
	if objectIdentifier == "" {
		objectIdentifier = RandomObjectIdentifier()
	}
	extensions := []string{"jpg", "tiff", "ogg", "txt", "xml", "pdf", "mp3", "mp4"}
	ext := RandomFromList(extensions)
	return fmt.Sprintf("%s/%s/%s.%s", objectIdentifier, fake.FemaleFirstName(), fake.ProductName(), ext)
}

func RandomFileFormat() string {
	formats := []string{"text/plain", "application/pdf", "audio/x-aac",
		"application/x-doom", "application/vnd.ms-excel"}
	return RandomFromList(formats)
}

func RandomEventType() string {
	return RandomFromList(constants.EventTypes)
}

func RandomAction() string {
	return RandomFromList(constants.ActionTypes)
}

func RandomStage() string {
	return RandomFromList(constants.StageTypes)
}

func RandomStatus() string {
	return RandomFromList(constants.StatusTypes)
}

func RandomFromList(items []string) string {
	i := int(math.Mod(float64(rand.Intn(200)), float64(len(items))))
	return items[i]
}

// Loads an IntellectualObject fixture (a JSON file) from
// the testdata directory for testing.
func LoadIntelObjFixture(filename string) (*models.IntellectualObject, error) {
	data, err := fileutil.LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	intelObj := &models.IntellectualObject{}
	err = json.Unmarshal(data, intelObj)
	if err != nil {
		return nil, err
	}
	return intelObj, nil
}

// GetContext returns a context object initialized with the specified
// config file. Param configFile should be the name of a JSON config
// file in the exchange/config directory. For example, "test.json"
// or "integration.json".
func GetContext(configFile string) (*context.Context, error) {
	configPath := filepath.Join("config", configFile)
	config, err := models.LoadConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	config.ExpandFilePaths()
	return context.NewContext(config), nil
}
