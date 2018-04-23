package transform

import (
	"github.com/m3db/m3coordinator/parser"
)

// Controller controls the caching and forwarding the request to downstream.
type Controller struct {
	ID         parser.NodeID
	transforms []parser.OpNode
}

// AddTransform adds a dependent transformation to the controller
func (t *Controller) AddTransform(node parser.OpNode) {
	t.transforms = append(t.transforms, node)
}
