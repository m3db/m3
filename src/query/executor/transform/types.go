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
	"errors"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errNoFetchOptionsSet      = errors.New("no fetch options set")
	errNoInstrumentOptionsSet = errors.New("no instrument options set")
)

// Options to create transform nodes.
type Options struct {
	fetchOpts         *storage.FetchOptions
	timeSpec          TimeSpec
	debug             bool
	blockType         models.FetchedBlockType
	instrumentOptions instrument.Options
}

// OptionsParams are the parameters used to create Options.
type OptionsParams struct {
	FetchOptions      *storage.FetchOptions
	TimeSpec          TimeSpec
	Debug             bool
	BlockType         models.FetchedBlockType
	InstrumentOptions instrument.Options
}

// NewOptions enforces that fields are set when options is created.
func NewOptions(p OptionsParams) (Options, error) {
	if p.FetchOptions == nil {
		return Options{}, errNoFetchOptionsSet
	}
	if p.InstrumentOptions == nil {
		return Options{}, errNoInstrumentOptionsSet
	}
	return Options{
		fetchOpts:         p.FetchOptions,
		timeSpec:          p.TimeSpec,
		debug:             p.Debug,
		blockType:         p.BlockType,
		instrumentOptions: p.InstrumentOptions,
	}, nil
}

// FetchOptions returns the FetchOptions option.
func (o Options) FetchOptions() *storage.FetchOptions {
	return o.fetchOpts
}

// TimeSpec returns the TimeSpec option.
func (o Options) TimeSpec() TimeSpec {
	return o.timeSpec
}

// Debug returns the Debug option.
func (o Options) Debug() bool {
	return o.debug
}

// BlockType returns the BlockType option.
func (o Options) BlockType() models.FetchedBlockType {
	return o.blockType
}

// InstrumentOptions returns the InstrumentOptions option.
func (o Options) InstrumentOptions() instrument.Options {
	return o.instrumentOptions
}

// OpNode represents an execution node.
type OpNode interface {
	Process(
		queryCtx *models.QueryContext,
		ID parser.NodeID,
		block block.Block,
	) error
}

// TimeSpec defines the time bounds for the query execution. Start is inclusive
// and End is exclusive.
type TimeSpec struct {
	// Start is the inclusive start bound for the query.
	Start xtime.UnixNano
	// End is the exclusive end bound for the query.
	End xtime.UnixNano
	// Now captures the current time and fixes it throughout the request.
	Now time.Time
	// Step is the step size for the query.
	Step time.Duration
}

// Bounds transforms the timespec to bounds.
func (ts TimeSpec) Bounds() models.Bounds {
	return models.Bounds{
		Start:    ts.Start,
		Duration: ts.End.Sub(ts.Start),
		StepSize: ts.Step,
	}
}

// Params are defined by transforms.
type Params interface {
	parser.Params
	Node(controller *Controller, opts Options) OpNode
}

// MetaNode is implemented by function nodes which
// can alter metadata for a block.
type MetaNode interface {
	// Meta provides the block metadata for the block using the
	// input blocks' metadata as input.
	Meta(meta block.Metadata) block.Metadata
	// SeriesMeta provides the series metadata for the block using the
	// previous blocks' series metadata as input.
	SeriesMeta(metas []block.SeriesMeta) []block.SeriesMeta
}

// BoundOp is an operation that is able to yield boundary information.
type BoundOp interface {
	Bounds() BoundSpec
}

// BoundSpec is the boundary specification for an operation.
type BoundSpec struct {
	// Range is the time range for the operation.
	Range time.Duration
	// Offset is the offset for the operation.
	Offset time.Duration
}
