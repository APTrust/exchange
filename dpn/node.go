package dpn

import (
	"math/rand"
	"time"
)

type Node struct {
	Name                 string       `json:"name"`
	Namespace            string       `json:"namespace"`
	APIRoot              string       `json:"api_root"`
	SSHPubKey            string       `json:"ssh_pubkey"`
	ReplicateFrom        []string     `json:"replicate_from"`
	ReplicateTo          []string     `json:"replicate_to"`
	RestoreFrom          []string     `json:"restore_from"`
	RestoreTo            []string     `json:"restore_to"`
	Protocols            []string     `json:"protocols"`
	FixityAlgorithms     []string     `json:"fixity_algorithms"`
	CreatedAt            time.Time    `json:"created_at"`
	UpdatedAt            time.Time    `json:"updated_at"`
	LastPullDate         time.Time    `json:"last_pull_date"`
	Storage              *Storage     `json:"storage"`
}


// This randomly chooses nodes for replication, returning
// a slice of strings. Each string is the namespace of a node
// we should replicate to. This may return fewer nodes than
// you specified in the howMany param if this node replicates
// to fewer nodes.
//
// We may have to revisit this in the future, if DPN specifies
// logic for how to choose remote nodes. For now, we can choose
// any node, because they are all geographically diverse and
// all use different storage backends.
func (node *Node) ChooseNodesForReplication(howMany int) ([]string) {
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
	return selectedNodes
}
