package models

import (
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

type Node struct {

	// Name is the full name of the node.
	Name string `json:"name"`

	// Namespace is the node's short name, and is generally
	// used as the node identifier on other record types.
	Namespace string `json:"namespace"`

	// APIRoot is the root URL of the node's DPN server.
	APIRoot string `json:"api_root"`

	// SSHPubKey is the public half of the SSH key that the
	// node uses to connect to other nodes to copy data via
	// rsync/ssh.
	SSHPubKey string `json:"ssh_pubkey"`

	// ReplicateFrom is a list of node namespaces from which
	// this node will replicate content.
	ReplicateFrom []string `json:"replicate_from"`

	// ReplicateTo is a list of node namespaces to which
	// this node will replicate content.
	ReplicateTo []string `json:"replicate_to"`

	// RestoreFrom is a list of node namespaces from which
	// this node will restore content.
	RestoreFrom []string `json:"restore_from"`

	// RestoreTo is a list of node namespaces to which
	// this node will restore content.
	RestoreTo []string `json:"restore_to"`

	// Protocols is a list of protocols this node supports for
	// copying files to and from other nodes. Initially, the
	// only supported protocol is rsync.
	Protocols []string `json:"protocols"`

	// FixityAlgorithms is a list of fixity algorithms this
	// node supports for calculating a bag's initial message
	// digest and subsequent periodic fixity checks. Initially,
	// all nodes support sha256.
	FixityAlgorithms []string `json:"fixity_algorithms"`

	// Storage describes the node's storage region and type.
	Storage *Storage `json:"storage"`

	// CreatedAt is the time at which this node record was
	// created in the DPN registry.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt describes when this node record was last
	// updated.
	UpdatedAt time.Time `json:"updated_at"`

	// LastPullDate describes when we last pulled data from
	// this node. This property is not part of the DPN spec.
	// It is for APTrust internal use only. We don't save
	// this info to the DPN registry, because there's no
	// place for it there. We save it locally and use it in
	// the dpn_sync chron job.
	LastPullDate time.Time `json:"last_pull_date"`
}

// ChooseNodesForReplication randomly chooses nodes for replication,
// returning a slice of strings. Each string is the namespace of a
// node we should replicate to. This may return fewer nodes than
// you specified in the howMany param if this node replicates
// to fewer nodes.
//
// We may have to revisit this in the future, if DPN specifies
// logic for how to choose remote nodes. For now, we can choose
// any node, because they are all geographically diverse and
// all use different storage backends.
//
// This will return an error if the number of nodes you want to select
// (the howMany param) exceeds the number of nodes that this node
// actually replicates to.
func (node *Node) ChooseNodesForReplication(howMany int) ([]string, error) {
	if howMany > len(node.ReplicateTo) {
		return nil, fmt.Errorf("Cannot choose %d nodes for replication when "+
			"we're only replicating to %d nodes.", howMany, len(node.ReplicateTo))
	}
	selectedNodes := make([]string, 0)
	if howMany >= len(node.ReplicateTo) {
		for _, namespace := range node.ReplicateTo {
			selectedNodes = append(selectedNodes, namespace)
		}
	} else {
		nodeMap := make(map[string]int)
		for len(selectedNodes) < howMany {
			randInt := rand.Intn(len(node.ReplicateTo))
			namespace := node.ReplicateTo[randInt]
			if _, alreadyAdded := nodeMap[namespace]; !alreadyAdded {
				selectedNodes = append(selectedNodes, namespace)
				nodeMap[namespace] = randInt
			}
		}
	}
	return selectedNodes, nil
}

// FQDN returns the fully-qualified domain name of this node's APIRoot.
// This will return an error if the APIRoot is not a valid URL
func (node *Node) FQDN() (string, error) {
	host := ""
	nodeUrl, err := url.Parse(node.APIRoot)
	if err == nil {
		host = nodeUrl.Host
		colonIndex := strings.Index(host, ":")
		if colonIndex > -1 {
			host = host[0:colonIndex]
		}
	}
	return host, err
}
