package dpn_test

// Common functions for dpn_test package

import (
	"fmt"
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/icrowley/fake"
	"github.com/satori/go.uuid"
	"time"
)

var fluctusUrl string = "http://localhost:3000"

// MakeDPNNode creates a mock DPN node object for testing.
func MakeDPNNode() (*dpn.Node) {
	return &dpn.Node{
		Name: fake.Word(),
		Namespace: fake.Word(),
		APIRoot: fmt.Sprintf("https://", fake.DomainName()),
		SSHPubKey: fake.Word(),
		CreatedAt: testutil.RandomDateTime(),
		UpdatedAt: testutil.RandomDateTime(),
		Protocols: []string{ "rsync" },
		Storage: &dpn.Storage{
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
func MakeDPNBag() (*dpn.DPNBag) {
	youyoueyedee := uuid.NewV4()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.DPNBag {
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
func MakeXferRequest(fromNode, toNode, bagUuid string) (*dpn.ReplicationTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.ReplicationTransfer{
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
func MakeRestoreRequest(fromNode, toNode, bagUuid string) (*dpn.RestoreTransfer) {
	id := uuid.NewV4()
	idString := id.String()
	tenSecondsAgo := time.Now().Add(-10 * time.Second)
	return &dpn.RestoreTransfer{
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

func MakeMessageDigest(bagUUID, node string) (*dpn.MessageDigest) {
	return &dpn.MessageDigest{
		Bag: bagUUID,
		Algorithm: "sha265",
		Node: node,
		Value: fake.CharactersN(64),
		CreatedAt: time.Now().UTC(),
	}
}

func MakeFixityCheck(bagUUID, node string) (*dpn.FixityCheck) {
	id := uuid.NewV4()
	idString := id.String()
	return &dpn.FixityCheck{
		FixityCheckId: idString,
		Bag: bagUUID,
		Node: node,
		Success: true,
		FixityAt: time.Now().UTC(),
		CreatedAt: time.Now().UTC(),
	}
}

// ---------------------------------------------------------------------
// Uncomment code below when we have the sync code in place
// ---------------------------------------------------------------------

// // This is the struct returned by AddRecords, so the caller can
// // know which records were created.
// type Mock struct {
//	DPNSync   *dpn.DPNSync
//	Bags      []*dpn.DPNBag
//	Xfers     []*dpn.ReplicationTransfer
//	Restores  []*dpn.RestoreTransfer
// }

// func NewMock(dpnSync *dpn.DPNSync) *Mock {
//	return &Mock{
//		DPNSync: dpnSync,
//	}
// }

// // Creates bags, transfer requests and restore requests
// // at the specified nodes.
// func (mock *Mock)AddRecordsToNodes(nodeNamespaces []string, count int) (err error) {
//	for _, node := range nodeNamespaces {
//		err = mock.AddRecordsToNode(node, count)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
// }

// // Create some bags, transfer requests and restore requests
// // at the specified node.
// func (mock *Mock)AddRecordsToNode(nodeNamespace string, count int) (err error) {
//	allNodes, err := mock.DPNSync.GetAllNodes()
//	if err != nil {
//		return fmt.Errorf("While adding records, " +
//			"can't get list of nodes: %v", err)
//	}
//	client := mock.DPNSync.RemoteClients[nodeNamespace]
//	if nodeNamespace == mock.DPNSync.LocalNodeName() {
//		client = mock.DPNSync.LocalClient
//	}
//	if client == nil {
//		return fmt.Errorf("No client available for node %s", nodeNamespace)
//	}
//	for i := 0; i < count; i++ {
//		// Create bags...
//		bag := MakeBag()
//		bag.IngestNode = nodeNamespace
//		bag.AdminNode = nodeNamespace
//		_, err = client.DPNBagCreate(bag)
//		if err != nil {
//			return err
//		}
//		mock.Bags = append(mock.Bags, bag)

//		for _, otherNode := range allNodes {
//			// Don't create transfers to the current node
//			if otherNode.Namespace == nodeNamespace {
//				continue
//			}
//			// Create replication transfers
//			xfer := MakeXferRequest(nodeNamespace,
//				otherNode.Namespace, bag.UUID)
//			_, err = client.ReplicationTransferCreate(xfer)
//			if err != nil {
//				return err
//			}
//			mock.Xfers = append(mock.Xfers, xfer)

//			// Create restore transfers
//			restore := MakeRestoreRequest(otherNode.Namespace,
//				nodeNamespace, bag.UUID)
//			_, err = client.RestoreTransferCreate(restore)
//			if err != nil {
//				return err
//			}
//			mock.Restores = append(mock.Restores, restore)
//		}
//	}
//	return nil
// }
