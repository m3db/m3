package util

import (
	"fmt"

	"github.com/m3db/m3em/node"
	m3dbnode "github.com/m3db/m3em/node/m3db"
)

// AsM3DBNodes returns casts a slice of ServiceNodes into M3DBNodes
func AsM3DBNodes(nodes []node.ServiceNode) ([]m3dbnode.Node, error) {
	m3dbnodes := make([]m3dbnode.Node, 0, len(nodes))
	for _, n := range nodes {
		mn, ok := n.(m3dbnode.Node)
		if !ok {
			return nil, fmt.Errorf("unable to cast: %v to m3dbnode", n.String())
		}
		m3dbnodes = append(m3dbnodes, mn)
	}
	return m3dbnodes, nil
}
