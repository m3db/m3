package transform

import (
	"time"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
)

// Options to create transform nodes
type Options struct {
	Now time.Time
}

// OpNode represents the execution node
type OpNode interface {
	Process(ID parser.NodeID, block storage.Block) error
}
