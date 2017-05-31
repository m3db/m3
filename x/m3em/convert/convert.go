package convert

import (
	"fmt"

	m3emnode "github.com/m3db/m3db/x/m3em/node"

	"github.com/m3db/m3em/node"
)

// AsM3DBNodes returns casts a slice of ServiceNodes into M3DBNodes
func AsM3DBNodes(nodes []node.ServiceNode) ([]m3emnode.Node, error) {
	m3dbnodes := make([]m3emnode.Node, 0, len(nodes))
	for _, n := range nodes {
		mn, ok := n.(m3emnode.Node)
		if !ok {
			return nil, fmt.Errorf("unable to cast: %v to m3dbnode", n.String())
		}
		m3dbnodes = append(m3dbnodes, mn)
	}
	return m3dbnodes, nil
}

// AsServiceNodes returns casts a slice M3DBNodes into ServiceNodes
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
