// Copyright (c) 2019 Uber Technologies, Inc.
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

package block

import (
	"time"

	"github.com/m3db/m3/src/query/ts"
)

type reconsolidatedBlock struct {
	block Block
}

// NewReconsolidatedBlock creates a lazy block wrapping another block with lazy options.
func NewReconsolidatedBlock(block Block) Block {
	return &reconsolidatedBlock{
		block: block,
	}
}

func (b *reconsolidatedBlock) Close() error { return b.block.Close() }

func (b *reconsolidatedBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (Block, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	return NewReconsolidatedBlock(bl), nil
}

// StepIter returns a StepIterator
func (b *reconsolidatedBlock) StepIter() (StepIter, error) {
	return b.block.StepIter()
}

// SeriesIter returns a SeriesIterator
func (b *reconsolidatedBlock) SeriesIter() (SeriesIter, error) {
	return b.block.SeriesIter()
}

// Unconsolidated returns the unconsolidated version for the block
func (b *reconsolidatedBlock) Unconsolidated() (UnconsolidatedBlock, error) {
	return &ucReconsolidatedBlock{
		block: b.block,
	}, nil
}

type ucReconsolidatedBlock struct {
	block Block
}

func (b *ucReconsolidatedBlock) Close() error { return b.block.Close() }

func (b *ucReconsolidatedBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (UnconsolidatedBlock, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	return &ucReconsolidatedBlock{
		block: bl,
	}, nil
}

func (b *ucReconsolidatedBlock) Consolidate() (Block, error) {
	return NewReconsolidatedBlock(b.block), nil
}

func (b *ucReconsolidatedBlock) StepIter() (UnconsolidatedStepIter, error) {
	iter, err := b.block.StepIter()
	if err != nil {
		return nil, err
	}

	return &ucReconsolidatedStepIter{
		it: iter,
		ts: iter.Meta().Bounds.Start,
	}, nil
}

type ucReconsolidatedStepIter struct {
	it StepIter
	ts time.Time
}

func (it *ucReconsolidatedStepIter) Close()                   { it.it.Close() }
func (it *ucReconsolidatedStepIter) Err() error               { return it.it.Err() }
func (it *ucReconsolidatedStepIter) StepCount() int           { return it.it.StepCount() }
func (it *ucReconsolidatedStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucReconsolidatedStepIter) Meta() Metadata           { return it.it.Meta() }

func (it *ucReconsolidatedStepIter) Next() bool {
	next := it.it.Next()
	it.ts = it.ts.Add(it.it.Meta().Bounds.StepSize)
	return next
}

func (it *ucReconsolidatedStepIter) Current() UnconsolidatedStep {
	var (
		c      = it.it.Current()
		values = c.Values()
		dpList = make([]ts.Datapoints, 0, len(values))
	)

	for _, val := range values {
		dpList = append(dpList, []ts.Datapoint{
			ts.Datapoint{
				Timestamp: it.ts,
				Value:     val,
			},
		},
		)
	}

	return unconsolidatedStep{
		time:   it.ts,
		values: dpList,
	}
}

func (b *ucReconsolidatedBlock) SeriesIter() (UnconsolidatedSeriesIter, error) {
	seriesIter, err := b.block.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &ucReconsolidatedSeriesIter{
		it:  seriesIter,
		idx: -1,
	}, nil
}

type ucReconsolidatedSeriesIter struct {
	it  SeriesIter
	idx int
}

func (it *ucReconsolidatedSeriesIter) Close()                   { it.it.Close() }
func (it *ucReconsolidatedSeriesIter) Err() error               { return it.it.Err() }
func (it *ucReconsolidatedSeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *ucReconsolidatedSeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucReconsolidatedSeriesIter) Meta() Metadata           { return it.it.Meta() }
func (it *ucReconsolidatedSeriesIter) Next() bool {
	next := it.it.Next()
	it.idx = it.idx + 1
	return next
}

func (it *ucReconsolidatedSeriesIter) Current() UnconsolidatedSeries {
	var (
		c      = it.it.Current()
		values = c.values
		dpList = make([]ts.Datapoints, 0, len(values))
		bounds = it.it.Meta().Bounds
		start  = bounds.Start
		step   = bounds.StepSize
	)

	for i, val := range values {
		dpList = append(dpList, ts.Datapoints{
			ts.Datapoint{
				Timestamp: start.Add(time.Duration(i) * step),
				Value:     val,
			},
		})
	}

	return UnconsolidatedSeries{
		datapoints: dpList,
		Meta:       it.SeriesMeta()[it.idx],
	}
}
