// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
