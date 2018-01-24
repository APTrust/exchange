package testutil

// Common functions for dpn_test package

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	apt_models "github.com/APTrust/exchange/models"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/icrowley/fake"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"math/rand"
	"net/url"
	"time"
)

var fluctusUrl string = "http://localhost:3000"

// BAG_IDS match the Bag.UUID values in the DPN server cluster
// fixture set at dpn-server/test/fixtures/integration.
var BAG_IDS = []string{
	"00000000-0000-4000-a000-000000000001",
	"00000000-0000-4000-a000-000000000002",
	"00000000-0000-4000-a000-000000000003",
	"00000000-0000-4000-a000-000000000004",
	"00000000-0000-4000-a000-000000000005",
}

// REPLICATION_IDS match ReplicationTransfer.ReplicationIds
// in the DPN cluster fixture set at dpn-server/test/fixtures/integration.
var REPLICATION_IDS = []string{
	"10000000-0000-4111-a000-000000000001",
	"10000000-0000-4111-a000-000000000007",
	"10000000-0000-4111-a000-000000000013",
	"10000000-0000-4111-a000-000000000019",
	"20000000-0000-4000-a000-000000000001",
	"20000000-0000-4000-a000-000000000007",
	"20000000-0000-4000-a000-000000000013",
	"20000000-0000-4000-a000-000000000019",
	"30000000-0000-4000-a000-000000000001",
	"30000000-0000-4000-a000-000000000007",
	"30000000-0000-4000-a000-000000000013",
	"30000000-0000-4000-a000-000000000019",
	"40000000-0000-4000-a000-000000000001",
	"40000000-0000-4000-a000-000000000007",
	"40000000-0000-4000-a000-000000000013",
	"40000000-0000-4000-a000-000000000019",
	"50000000-0000-4000-a000-000000000001",
	"50000000-0000-4000-a000-000000000007",
	"50000000-0000-4000-a000-000000000013",
	"50000000-0000-4000-a000-000000000019",
}

// RESTORE_IDS match RestorationTransfer.RestoreIds
// in the DPN cluster fixture set at dpn-server/test/fixtures/integration.
var RESTORE_IDS = []string{
	"11000000-0000-4111-a000-000000000001",
	"11000000-0000-4111-a000-000000000002",
	"11000000-0000-4111-a000-000000000003",
	"11000000-0000-4111-a000-000000000004",
	"21000000-0000-4111-a000-000000000001",
	"21000000-0000-4111-a000-000000000002",
	"21000000-0000-4111-a000-000000000003",
	"21000000-0000-4111-a000-000000000004",
	"31000000-0000-4111-a000-000000000001",
	"31000000-0000-4111-a000-000000000002",
	"31000000-0000-4111-a000-000000000003",
	"31000000-0000-4111-a000-000000000004",
	"41000000-0000-4111-a000-000000000001",
	"41000000-0000-4111-a000-000000000002",
	"41000000-0000-4111-a000-000000000003",
	"41000000-0000-4111-a000-000000000004",
	"51000000-0000-4111-a000-000000000001",
	"51000000-0000-4111-a000-000000000002",
	"51000000-0000-4111-a000-000000000003",
	"51000000-0000-4111-a000-000000000004",
}

// MakeDPNNode creates a mock DPN node object for testing.
func MakeDPNNode() *models.Node {
	return &models.Node{
		Name:      fake.Word(),
		Namespace: fake.Word(),
		APIRoot:   fmt.Sprintf("https://%s", fake.DomainName()),
		SSHPubKey: fake.Word(),
		CreatedAt: apt_testutil.RandomDateTime(),
		UpdatedAt: apt_testutil.RandomDateTime(),
		Protocols: []string{"rsync"},
		Storage: &models.Storage{
			Region: "palookaville",
			Type:   "shoe box",
		},
		FixityAlgorithms: []string{"sha256"},
		ReplicateFrom:    []string{"aptrust", "chron", "sdr", "tdr"},
		ReplicateTo:      []string{"aptrust", "chron", "sdr", "tdr"},
		RestoreFrom:      []string{"aptrust", "chron", "sdr", "tdr"},
		RestoreTo:        []string{"aptrust", "chron", "sdr", "tdr"},
	}
}

// MakeDPNBag creates a mock DPN bag for testing.
func MakeDPNBag() *models.DPNBag {
	youyoueyedee := uuid.NewV4()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.DPNBag{
		UUID:             youyoueyedee.String(),
		Interpretive:     []string{},
		Rights:           []string{},
		ReplicatingNodes: []string{},
		LocalId:          fmt.Sprintf("GO-TEST-BAG-%s", youyoueyedee.String()),
		Size:             12345678,
		FirstVersionUUID: youyoueyedee.String(),
		Version:          1,
		BagType:          "D",
		IngestNode:       "aptrust",
		AdminNode:        "aptrust",
		Member:           "9a000000-0000-4000-a000-000000000001", // Sunnyvale College
		CreatedAt:        tenSecondsAgo,
		UpdatedAt:        tenSecondsAgo,
	}
}

// MakeXferRequest creates a DPN ReplicationTransfer object.
func MakeXferRequest(fromNode, toNode, bagUuid string) *models.ReplicationTransfer {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.ReplicationTransfer{
		FromNode:        fromNode,
		ToNode:          toNode,
		Bag:             bagUuid,
		ReplicationId:   uuid.NewV4().String(),
		FixityAlgorithm: "sha256",
		FixityNonce:     nil,
		FixityValue:     nil,
		Protocol:        "rsync",
		Link:            fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt:       tenSecondsAgo,
		UpdatedAt:       tenSecondsAgo,
	}
}

// MakeRestoreRequest creates a DPN RestoreTransfer object.
func MakeRestoreRequest(fromNode, toNode, bagUuid string) *models.RestoreTransfer {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.RestoreTransfer{
		FromNode:  fromNode,
		ToNode:    toNode,
		Bag:       bagUuid,
		RestoreId: uuid.NewV4().String(),
		Protocol:  "rsync",
		Link:      fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// MakeMessageDigest creates a DPN MessageDigest object for testing.
func MakeMessageDigest(bagUUID, node string) *models.MessageDigest {
	return &models.MessageDigest{
		Bag:       bagUUID,
		Algorithm: "sha256",
		Node:      node,
		Value:     fake.CharactersN(64),
		CreatedAt: time.Now().UTC(),
	}
}

// MakeFixityCheck creates a DPN FixityCheck object for testing.
func MakeFixityCheck(bagUUID, node string) *models.FixityCheck {
	id := uuid.NewV4()
	idString := id.String()
	return &models.FixityCheck{
		FixityCheckId: idString,
		Bag:           bagUUID,
		Node:          node,
		Success:       true,
		FixityAt:      time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
	}
}

// MakeIngest creates a new DPN Ingest object for testing.
func MakeIngest(bagUUID string) *models.Ingest {
	id := uuid.NewV4()
	idString := id.String()
	return &models.Ingest{
		IngestId:         idString,
		Bag:              bagUUID,
		Ingested:         true,
		ReplicatingNodes: []string{"tdr", "sdr"},
		CreatedAt:        time.Now().UTC(),
	}
}

func MakeDPNStoredFile() *models.DPNStoredFile {
	now := time.Now().UTC()
	return &models.DPNStoredFile{
		Id:           int64(rand.Intn(100000)),
		Key:          uuid.NewV4().String(),
		Bucket:       fake.Word(),
		Size:         int64(rand.Intn(900000000)),
		ContentType:  fake.Word(),
		Member:       fake.Word(),
		FromNode:     fake.Word(),
		TransferId:   fake.Word(),
		LocalId:      fake.Word(),
		Version:      fake.Word(),
		ETag:         fake.Word(),
		LastModified: now,
		LastSeenAt:   now,
		CreatedAt:    now,
		UpdatedAt:    now,
		DeletedAt:    now,
	}
}

// MakeNsqMessage creates an NSQ Message with the specified body.
// For our purposes, param body should be an integer in string format,
// like "1234" or "999".
func MakeNsqMessage(body string) *nsq.Message {
	messageId := [nsq.MsgIDLength]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'}
	return nsq.NewMessage(messageId, []byte(body))
}

// GetDPNWorkItems returns WorkItems whose action="DPN". This is
// used in a number of DPN post tests in the integration/ directory.
func GetDPNWorkItems() (*context.Context, []*apt_models.WorkItem, error) {
	_context, err := apt_testutil.GetContext("integration.json")
	if err != nil {
		return nil, nil, err
	}
	params := url.Values{}
	params.Set("item_action", "DPN")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.WorkItemList(params)
	return _context, resp.WorkItems(), resp.Error
}
