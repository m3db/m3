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

type lazyBlock struct {
	block Block
	opts  LazyOptions
}

// NewLazyBlock creates a lazy block wrapping another block with lazy options.
func NewLazyBlock(block Block, opts LazyOptions) Block {
	return &lazyBlock{
		block: block,
		opts:  opts,
	}
}

func (b *lazyBlock) Close() error { return b.block.Close() }

func (b *lazyBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (Block, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	return NewLazyBlock(bl, b.opts), nil
}

// StepIter returns a StepIterator
func (b *lazyBlock) StepIter() (StepIter, error) {
	iter, err := b.block.StepIter()
	if err != nil {
		return nil, err
	}

	return &lazyStepIter{
		it:   iter,
		opts: b.opts,
	}, nil
}

type lazyStepIter struct {
	it   StepIter
	opts LazyOptions
}

func (it *lazyStepIter) Close()                   { it.it.Close() }
func (it *lazyStepIter) Err() error               { return it.it.Err() }
func (it *lazyStepIter) StepCount() int           { return it.it.StepCount() }
func (it *lazyStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *lazyStepIter) Next() bool               { return it.it.Next() }

func (it *lazyStepIter) Meta() Metadata {
	mt := it.opts.MetaTransform()
	return mt(it.it.Meta())
}

func (it *lazyStepIter) Current() Step {
	var (
		c        = it.it.Current()
		tt, vt   = it.opts.TimeTransform(), it.opts.ValueTransform()
		stepVals = c.Values()
	)

	vals := make([]float64, 0, len(stepVals))
	for _, val := range stepVals {
		vals = append(vals, vt(val))
	}

	return ColStep{
		time:   tt(c.Time()),
		values: vals,
	}
}

// SeriesIter returns a SeriesIterator
func (b *lazyBlock) SeriesIter() (SeriesIter, error) {
	iter, err := b.block.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &lazySeriesIter{
		it:   iter,
		opts: b.opts,
	}, nil
}

type lazySeriesIter struct {
	it   SeriesIter
	opts LazyOptions
}

func (it *lazySeriesIter) Close()                   { it.it.Close() }
func (it *lazySeriesIter) Err() error               { return it.it.Err() }
func (it *lazySeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *lazySeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *lazySeriesIter) Next() bool               { return it.it.Next() }

func (it *lazySeriesIter) Current() Series {
	var (
		c    = it.it.Current()
		vt   = it.opts.ValueTransform()
		vals = make([]float64, 0, len(c.values))
	)

	for _, val := range c.values {
		vals = append(vals, vt(val))
	}

	c.values = vals
	return c
}

func (it *lazySeriesIter) Meta() Metadata {
	mt := it.opts.MetaTransform()
	return mt(it.it.Meta())
}

// Unconsolidated returns the unconsolidated version for the block
func (b *lazyBlock) Unconsolidated() (UnconsolidatedBlock, error) {
	unconsolidated, err := b.block.Unconsolidated()
	if err != nil {
		return nil, err
	}

	return &ucLazyBlock{
		block: unconsolidated,
		opts:  b.opts,
	}, nil
}

type ucLazyBlock struct {
	block UnconsolidatedBlock
	opts  LazyOptions
}

func (b *ucLazyBlock) Close() error { return b.block.Close() }

func (b *ucLazyBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (UnconsolidatedBlock, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	return &ucLazyBlock{
		block: bl,
		opts:  b.opts,
	}, nil
}

func (b *ucLazyBlock) Consolidate() (Block, error) {
	block, err := b.block.Consolidate()
	if err != nil {
		return nil, err
	}

	return &lazyBlock{
		block: block,
		opts:  b.opts,
	}, nil
}

func (b *ucLazyBlock) StepIter() (UnconsolidatedStepIter, error) {
	iter, err := b.block.StepIter()
	if err != nil {
		return nil, err
	}

	return &ucLazyStepIter{
		it:   iter,
		opts: b.opts,
	}, nil
}

type ucLazyStepIter struct {
	it   UnconsolidatedStepIter
	opts LazyOptions
}

func (it *ucLazyStepIter) Close()                   { it.it.Close() }
func (it *ucLazyStepIter) Err() error               { return it.it.Err() }
func (it *ucLazyStepIter) StepCount() int           { return it.it.StepCount() }
func (it *ucLazyStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucLazyStepIter) Next() bool               { return it.it.Next() }

func (it *ucLazyStepIter) Meta() Metadata {
	mt := it.opts.MetaTransform()
	return mt(it.it.Meta())
}

type unconsolidatedStep struct {
	time   time.Time
	values []ts.Datapoints
}

// Time for the step.
func (s unconsolidatedStep) Time() time.Time {
	return s.time
}

// Values for the column.
func (s unconsolidatedStep) Values() []ts.Datapoints {
	return s.values
}

func (it *ucLazyStepIter) Current() UnconsolidatedStep {
	var (
		c      = it.it.Current()
		values = c.Values()
		dpList = make([]ts.Datapoints, 0, len(values))
		tt, vt = it.opts.TimeTransform(), it.opts.ValueTransform()
	)

	for _, val := range values {
		dps := make([]ts.Datapoint, 0, len(val))
		for _, dp := range val.Datapoints() {
			dps = append(dps, ts.Datapoint{
				Timestamp: tt(dp.Timestamp),
				Value:     vt(dp.Value),
			})
		}

		dpList = append(dpList, dps)
	}

	return unconsolidatedStep{
		time:   tt(c.Time()),
		values: dpList,
	}
}

func (b *ucLazyBlock) SeriesIter() (UnconsolidatedSeriesIter, error) {
	seriesIter, err := b.block.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &ucLazySeriesIter{
		it:   seriesIter,
		opts: b.opts,
	}, nil
}

type ucLazySeriesIter struct {
	it   UnconsolidatedSeriesIter
	opts LazyOptions
}

func (it *ucLazySeriesIter) Close()                   { it.it.Close() }
func (it *ucLazySeriesIter) Err() error               { return it.it.Err() }
func (it *ucLazySeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *ucLazySeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucLazySeriesIter) Next() bool               { return it.it.Next() }

func (it *ucLazySeriesIter) Current() UnconsolidatedSeries {
	var (
		c      = it.it.Current()
		values = c.datapoints
		dpList = make([]ts.Datapoints, 0, len(values))
		tt, vt = it.opts.TimeTransform(), it.opts.ValueTransform()
	)

	for _, val := range values {
		dps := make([]ts.Datapoint, 0, len(val))
		for _, dp := range val.Datapoints() {
			dps = append(dps, ts.Datapoint{
				Timestamp: tt(dp.Timestamp),
				Value:     vt(dp.Value),
			})
		}

		dpList = append(dpList, dps)
	}

	c.datapoints = dpList
	return c
}

func (it *ucLazySeriesIter) Meta() Metadata {
	mt := it.opts.MetaTransform()
	return mt(it.it.Meta())
}
