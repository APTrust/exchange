package testutil

// Common functions for dpn_test package

import (
	"fmt"
	"github.com/APTrust/exchange/dpn/models"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/icrowley/fake"
	"github.com/satori/go.uuid"
	"time"
)

var fluctusUrl string = "http://localhost:3000"

// MakeDPNNode creates a mock DPN node object for testing.
func MakeDPNNode() (*models.Node) {
	return &models.Node{
		Name: fake.Word(),
		Namespace: fake.Word(),
		APIRoot: fmt.Sprintf("https://", fake.DomainName()),
		SSHPubKey: fake.Word(),
		CreatedAt: apt_testutil.RandomDateTime(),
		UpdatedAt: apt_testutil.RandomDateTime(),
		Protocols: []string{ "rsync" },
		Storage: &models.Storage{
			Region: "palookaville",
			Type: "shoe box",
		},
		FixityAlgorithms: []string{ "sha256" },
		ReplicateFrom: []string{ "aptrust", "chron", "sdr", "tdr" },
		ReplicateTo: []string{ "aptrust", "chron", "sdr", "tdr" },
		RestoreFrom: []string{ "aptrust", "chron", "sdr", "tdr" },
		RestoreTo: []string{ "aptrust", "chron", "sdr", "tdr" },
	}
}

// MakeDPNBag creates a mock DPN bag for testing.
func MakeDPNBag() (*models.DPNBag) {
	youyoueyedee := uuid.NewV4()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.DPNBag {
		UUID: youyoueyedee.String(),
		Interpretive: []string{},
		Rights: []string{},
		ReplicatingNodes: []string{},
		LocalId: fmt.Sprintf("GO-TEST-BAG-%s", youyoueyedee.String()),
		Size: 12345678,
		FirstVersionUUID: youyoueyedee.String(),
		Version: 1,
		BagType: "D",
		IngestNode: "aptrust",
		AdminNode: "aptrust",
		Member: "9a000000-0000-4000-a000-000000000002", // Faber College
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// Creates a DPN replication transfer object.
func MakeXferRequest(fromNode, toNode, bagUuid string) (*models.ReplicationTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.ReplicationTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		Bag: bagUuid,
		ReplicationId: uuid.NewV4().String(),
		FixityAlgorithm: "sha256",
		FixityNonce: nil,
		FixityValue: nil,
		Protocol: "rsync",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

// Creates a DPN restore transfer object.
func MakeRestoreRequest(fromNode, toNode, bagUuid string) (*models.RestoreTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &models.RestoreTransfer{
		FromNode: fromNode,
		ToNode: toNode,
		Bag: bagUuid,
		RestoreId: uuid.NewV4().String(),
		Protocol: "rsync",
		Link: fmt.Sprintf("rsync://mnt/staging/%s.tar", idString),
		CreatedAt: tenSecondsAgo,
		UpdatedAt: tenSecondsAgo,
	}
}

func MakeMessageDigest(bagUUID, node string) (*models.MessageDigest) {
	return &models.MessageDigest{
		Bag: bagUUID,
		Algorithm: "sha256",
		Node: node,
		Value: fake.CharactersN(64),
		CreatedAt: time.Now().UTC(),
	}
}

func MakeFixityCheck(bagUUID, node string) (*models.FixityCheck) {
	id := uuid.NewV4()
	idString := id.String()
	return &models.FixityCheck{
		FixityCheckId: idString,
		Bag: bagUUID,
		Node: node,
		Success: true,
		FixityAt: time.Now().UTC(),
		CreatedAt: time.Now().UTC(),
	}
}

func MakeIngest(bagUUID string) (*models.Ingest) {
	id := uuid.NewV4()
	idString := id.String()
	return &models.Ingest{
		IngestId: idString,
		Bag: bagUUID,
		Ingested: true,
		ReplicatingNodes: []string { "tdr", "sdr" },
		CreatedAt: time.Now().UTC(),
	}
}

// Creates an NSQ Message with the specified body. For our
// purposes, param body should be an integer in string format,
// like "1234" or "999".
func MakeNsqMessage(body string) (*nsq.Message) {
	messageId := [nsq.MsgIDLength]byte{'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'}
	return nsq.NewMessage(messageId, []byte(body))
}
