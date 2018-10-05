package convert

import (
	"fmt"

	m3emnode "github.com/m3db/m3/src/dbnode/x/m3em/node"
	"github.com/m3db/m3/src/m3em/node"
)

// AsNodes returns casts a slice of ServiceNodes into m3emnode.Nodes
func AsNodes(nodes []node.ServiceNode) ([]m3emnode.Node, error) {
	m3emnodes := make([]m3emnode.Node, 0, len(nodes))
	for _, n := range nodes {
		mn, ok := n.(m3emnode.Node)
		if !ok {
			return nil, fmt.Errorf("unable to cast: %v to m3dbnode", n.String())
		}
		m3emnodes = append(m3emnodes, mn)
	}
	return m3emnodes, nil
}

// AsServiceNodes returns casts a slice m3emnode.Nodes into ServiceNodes
func AsServiceNodes(nodes []m3emnode.Node) ([]node.ServiceNode, error) {
	serviceNodes := make([]node.ServiceNode, 0, len(nodes))
	for _, mn := range nodes {
		n, ok := mn.(node.ServiceNode)
		if !ok {
			return nil, fmt.Errorf("unable to cast: %v to ServiceNode", n.String())
		}
		serviceNodes = append(serviceNodes, n)
	}
	return serviceNodes, nil
}
