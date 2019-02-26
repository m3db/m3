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

package transform

import (
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

// Options to create transform nodes
type Options struct {
	TimeSpec  TimeSpec
	Debug     bool
	BlockType models.FetchedBlockType
}

// OpNode represents the execution node
type OpNode interface {
	Process(queryCtx *models.QueryContext, ID parser.NodeID, block block.Block) error
}

// TimeSpec defines the time bounds for the query execution. End is exclusive
type TimeSpec struct {
	Start time.Time
	End   time.Time
	// Now captures the current time and fixes it throughout the request, we may let people override it in the future
	Now  time.Time
	Step time.Duration
}

// Bounds transforms a timespec to bounds
func (ts TimeSpec) Bounds() models.Bounds {
	return models.Bounds{
		Start:    ts.Start,
		Duration: ts.End.Sub(ts.Start),
		StepSize: ts.Step,
	}
}

// Params are defined by transforms
type Params interface {
	parser.Params
	Node(controller *Controller, opts Options) OpNode
}

// MetaNode is implemented by function nodes which can alter metadata for a block
type MetaNode interface {
	// Meta provides the block metadata for the block using the input blocks' metadata as input
	Meta(meta block.Metadata) block.Metadata
	// SeriesMeta provides the series metadata for the block using the previous blocks' series metadata as input
	SeriesMeta(metas []block.SeriesMeta) []block.SeriesMeta
}

// SeriesNode is implemented by function nodes which can support series iteration
type SeriesNode interface {
	MetaNode
	ProcessSeries(series block.Series) (block.Series, error)
}

// StepNode is implemented by function nodes which can support step iteration
type StepNode interface {
	MetaNode
	ProcessStep(step block.Step) (block.Step, error)
}

// BoundOp is implements by operations which have bounds
type BoundOp interface {
	Bounds() BoundSpec
}

// BoundSpec is the bound spec for an operation
type BoundSpec struct {
	Range  time.Duration
	Offset time.Duration
}
