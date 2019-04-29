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

package block

import (
	"time"

	"github.com/m3db/m3/src/query/ts"
)

func updateMeta(meta Metadata, offset time.Duration) Metadata {
	meta.Bounds.Start = meta.Bounds.Start.Add(offset)
	return meta
}

type offsetBlock struct {
	block  Block
	offset time.Duration
}

// NewOffsetBlock creates an offset block wrapping another block with an offset.
func NewOffsetBlock(block Block, offset time.Duration) Block {
	// NB: this is an invalid case; however, if offset is invalid, it's safe to
	// return the base block instead.
	if offset <= 0 {
		return block
	}

	return &offsetBlock{
		block:  block,
		offset: offset,
	}
}

func (b *offsetBlock) Close() error { return b.block.Close() }

func (b *offsetBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (Block, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	b.block = bl
	return b, nil
}

// StepIter returns a StepIterator
func (b *offsetBlock) StepIter() (StepIter, error) {
	iter, err := b.block.StepIter()
	if err != nil {
		return nil, err
	}

	return &offsetStepIter{
		it:     iter,
		offset: b.offset,
	}, nil
}

type offsetStepIter struct {
	it     StepIter
	offset time.Duration
}

func (it *offsetStepIter) Close()                   { it.it.Close() }
func (it *offsetStepIter) Err() error               { return it.it.Err() }
func (it *offsetStepIter) StepCount() int           { return it.it.StepCount() }
func (it *offsetStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *offsetStepIter) Next() bool               { return it.it.Next() }

func (it *offsetStepIter) Meta() Metadata {
	return updateMeta(it.it.Meta(), it.offset)
}

func (it *offsetStepIter) Current() Step {
	c := it.it.Current()
	return ColStep{
		time:   c.Time().Add(it.offset),
		values: c.Values(),
	}
}

// SeriesIter returns a SeriesIterator
func (b *offsetBlock) SeriesIter() (SeriesIter, error) {
	iter, err := b.block.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &offsetSeriesIter{
		it:     iter,
		offset: b.offset,
	}, nil
}

type offsetSeriesIter struct {
	it     SeriesIter
	offset time.Duration
}

func (it *offsetSeriesIter) Close()                   { it.it.Close() }
func (it *offsetSeriesIter) Err() error               { return it.it.Err() }
func (it *offsetSeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *offsetSeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *offsetSeriesIter) Next() bool               { return it.it.Next() }
func (it *offsetSeriesIter) Current() Series          { return it.it.Current() }
func (it *offsetSeriesIter) Meta() Metadata {
	return updateMeta(it.it.Meta(), it.offset)
}

// Unconsolidated returns the unconsolidated version for the block
func (b *offsetBlock) Unconsolidated() (UnconsolidatedBlock, error) {
	unconsolidated, err := b.block.Unconsolidated()
	if err != nil {
		return nil, err
	}

	return &ucOffsetBlock{
		block:  unconsolidated,
		offset: b.offset,
	}, nil
}

type ucOffsetBlock struct {
	block  UnconsolidatedBlock
	offset time.Duration
}

func (b *ucOffsetBlock) Close() error { return b.block.Close() }

func (b *ucOffsetBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (UnconsolidatedBlock, error) {
	bl, err := b.block.WithMetadata(meta, sm)
	if err != nil {
		return nil, err
	}

	b.block = bl
	return b, nil
}

func (b *ucOffsetBlock) Consolidate() (Block, error) {
	block, err := b.block.Consolidate()
	if err != nil {
		return nil, err
	}

	return &offsetBlock{
		block:  block,
		offset: b.offset,
	}, nil
}

func (b *ucOffsetBlock) StepIter() (UnconsolidatedStepIter, error) {
	iter, err := b.block.StepIter()
	if err != nil {
		return nil, err
	}

	return &ucOffsetStepIter{
		it:     iter,
		offset: b.offset,
	}, nil
}

type ucOffsetStepIter struct {
	it     UnconsolidatedStepIter
	offset time.Duration
}

func (it *ucOffsetStepIter) Close()                   { it.it.Close() }
func (it *ucOffsetStepIter) Err() error               { return it.it.Err() }
func (it *ucOffsetStepIter) StepCount() int           { return it.it.StepCount() }
func (it *ucOffsetStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucOffsetStepIter) Next() bool               { return it.it.Next() }

func (it *ucOffsetStepIter) Meta() Metadata {
	return updateMeta(it.it.Meta(), it.offset)
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

func (it *ucOffsetStepIter) Current() UnconsolidatedStep {
	c := it.it.Current()
	for _, val := range c.Values() {
		for i, dp := range val.Datapoints() {
			val[i].Timestamp = dp.Timestamp.Add(it.offset)
		}
	}

	return unconsolidatedStep{
		time:   c.Time().Add(it.offset),
		values: c.Values(),
	}
}

func (b *ucOffsetBlock) SeriesIter() (UnconsolidatedSeriesIter, error) {
	seriesIter, err := b.block.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &ucOffsetSeriesIter{
		it:     seriesIter,
		offset: b.offset,
	}, nil
}

type ucOffsetSeriesIter struct {
	it     UnconsolidatedSeriesIter
	offset time.Duration
}

func (it *ucOffsetSeriesIter) Close()                   { it.it.Close() }
func (it *ucOffsetSeriesIter) Err() error               { return it.it.Err() }
func (it *ucOffsetSeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *ucOffsetSeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucOffsetSeriesIter) Next() bool               { return it.it.Next() }
func (it *ucOffsetSeriesIter) Current() UnconsolidatedSeries {
	c := it.it.Current()
	for _, val := range c.datapoints {
		for i, dp := range val.Datapoints() {
			val[i].Timestamp = dp.Timestamp.Add(it.offset)
		}
	}

	return c
}

func (it *ucOffsetSeriesIter) Meta() Metadata {
	return updateMeta(it.it.Meta(), it.offset)
}
