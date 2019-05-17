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

var (
	defaultTimeTransform  = func(t time.Time) time.Time { return t }
	defaultMetaTransform  = func(meta Metadata) Metadata { return meta }
	defaultValueTransform = func(val float64) float64 { return val }
)

type lazyOpts struct {
	timeTransform  TimeTransform
	metaTransform  MetaTransform
	valueTransform ValueTransform
}

// NewLazyOpts creates LazyOpts with default values
func NewLazyOpts() LazyOpts {
	return &lazyOpts{
		timeTransform:  defaultTimeTransform,
		metaTransform:  defaultMetaTransform,
		valueTransform: defaultValueTransform,
	}
}

func (o *lazyOpts) SetTimeTransform(tt TimeTransform) LazyOpts {
	opts := *o
	opts.timeTransform = tt
	return &opts
}

func (o *lazyOpts) TimeTransform() TimeTransform {
	return o.timeTransform
}

func (o *lazyOpts) SetMetaTransform(mt MetaTransform) LazyOpts {
	opts := *o
	opts.metaTransform = mt
	return &opts
}

func (o *lazyOpts) MetaTransform() MetaTransform {
	return o.metaTransform
}

func (o *lazyOpts) SetValueTransform(vt ValueTransform) LazyOpts {
	opts := *o
	opts.valueTransform = vt
	return &opts
}

func (o *lazyOpts) ValueTransform() ValueTransform {
	return o.valueTransform
}

type lazyBlock struct {
	block Block
	opts  LazyOpts
}

// NewLazyBlock creates a lazy block wrapping another block with lazy options.
func NewLazyBlock(block Block, opts LazyOpts) Block {
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

	b.block = bl
	return b, nil
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
	opts LazyOpts
}

func (it *lazyStepIter) Close()                   { it.it.Close() }
func (it *lazyStepIter) Err() error               { return it.it.Err() }
func (it *lazyStepIter) StepCount() int           { return it.it.StepCount() }
func (it *lazyStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *lazyStepIter) Next() bool               { return it.it.Next() }

func (it *lazyStepIter) Meta() Metadata {
	return it.opts.MetaTransform()(it.it.Meta())
}

func (it *lazyStepIter) Current() Step {
	c := it.it.Current()
	return ColStep{
		time:   it.opts.TimeTransform()(c.Time()),
		values: c.Values(),
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
	opts LazyOpts
}

func (it *lazySeriesIter) Close()                   { it.it.Close() }
func (it *lazySeriesIter) Err() error               { return it.it.Err() }
func (it *lazySeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *lazySeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *lazySeriesIter) Next() bool               { return it.it.Next() }
func (it *lazySeriesIter) Current() Series          { return it.it.Current() }
func (it *lazySeriesIter) Meta() Metadata {
	return it.opts.MetaTransform()(it.it.Meta())
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
	opts  LazyOpts
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

	b.block = bl
	return b, nil
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
	opts LazyOpts
}

func (it *ucLazyStepIter) Close()                   { it.it.Close() }
func (it *ucLazyStepIter) Err() error               { return it.it.Err() }
func (it *ucLazyStepIter) StepCount() int           { return it.it.StepCount() }
func (it *ucLazyStepIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucLazyStepIter) Next() bool               { return it.it.Next() }

func (it *ucLazyStepIter) Meta() Metadata {
	return it.opts.MetaTransform()(it.it.Meta())
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
	c := it.it.Current()
	for _, val := range c.Values() {
		for i, dp := range val.Datapoints() {
			val[i].Timestamp = it.opts.TimeTransform()(dp.Timestamp)
			val[i].Value = it.opts.ValueTransform()(dp.Value)
		}
	}

	return unconsolidatedStep{
		time:   it.opts.TimeTransform()(c.Time()),
		values: c.Values(),
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
	opts LazyOpts
}

func (it *ucLazySeriesIter) Close()                   { it.it.Close() }
func (it *ucLazySeriesIter) Err() error               { return it.it.Err() }
func (it *ucLazySeriesIter) SeriesCount() int         { return it.it.SeriesCount() }
func (it *ucLazySeriesIter) SeriesMeta() []SeriesMeta { return it.it.SeriesMeta() }
func (it *ucLazySeriesIter) Next() bool               { return it.it.Next() }
func (it *ucLazySeriesIter) Current() UnconsolidatedSeries {
	c := it.it.Current()
	for _, val := range c.datapoints {
		for i, dp := range val.Datapoints() {
			val[i].Timestamp = it.opts.TimeTransform()(dp.Timestamp)
			val[i].Value = it.opts.ValueTransform()(dp.Value)
		}
	}

	return c
}

func (it *ucLazySeriesIter) Meta() Metadata {
	return it.opts.MetaTransform()(it.it.Meta())
}
