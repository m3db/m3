package types

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
)

// ReadResult is a result from a remote read.
type ReadResult struct {
	Series    []*ts.Series
	Meta      block.ResultMetadata
	BlockType block.BlockType
}
