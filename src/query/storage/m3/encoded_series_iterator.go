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

package m3

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
)

// NewEncodedSeriesIter creates a new encoded series iterator.
func NewEncodedSeriesIter(
	meta block.Metadata,
	seriesMetas []block.SeriesMeta,
	seriesIters []encoding.SeriesIterator,
	instrumented bool,
) block.SeriesIter {
	return &encodedSeriesIter{
		idx:          -1,
		meta:         meta,
		seriesMeta:   seriesMetas,
		seriesIters:  seriesIters,
		instrumented: instrumented,
	}
}

type encodedSeriesIter struct {
	idx          int
	err          error
	meta         block.Metadata
	datapoints   ts.Datapoints
	series       block.UnconsolidatedSeries
	seriesMeta   []block.SeriesMeta
	seriesIters  []encoding.SeriesIterator
	instrumented bool
}

func (it *encodedSeriesIter) Current() block.UnconsolidatedSeries {
	return it.series
}

func (it *encodedSeriesIter) Err() error {
	return it.err
}

func (it *encodedSeriesIter) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < len(it.seriesIters)
	if !next {
		return false
	}

	iter := it.seriesIters[it.idx]
	if it.datapoints == nil {
		it.datapoints = make(ts.Datapoints, 0, initBlockReplicaLength)
	} else {
		it.datapoints = it.datapoints[:0]
	}

	var (
		decodeDuration time.Duration
		decodeStart    time.Time
	)
	if it.instrumented {
		decodeStart = time.Now()
	}

	for iter.Next() {
		dp, _, _ := iter.Current()
		it.datapoints = append(it.datapoints,
			ts.Datapoint{
				Timestamp: dp.TimestampNanos,
				Value:     dp.Value,
			})
	}

	if it.instrumented {
		decodeDuration = time.Since(decodeStart)
	}

	if it.err = iter.Err(); it.err != nil {
		return false
	}

	it.series = block.NewUnconsolidatedSeries(
		it.datapoints,
		it.seriesMeta[it.idx],
		block.UnconsolidatedSeriesStats{
			Enabled:        it.instrumented,
			DecodeDuration: decodeDuration,
		})

	return next
}

func (it *encodedSeriesIter) SeriesCount() int {
	return len(it.seriesIters)
}

func (it *encodedSeriesIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedSeriesIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}

// MultiSeriesIter returns batched series iterators for the block based on
// given concurrency.
func (b *encodedBlock) MultiSeriesIter(
	concurrency int,
) ([]block.SeriesIterBatch, error) {
	fn := iteratorBatchingFn
	if b.options != nil && b.options.IteratorBatchingFn() != nil {
		fn = b.options.IteratorBatchingFn()
	}

	return fn(
		concurrency,
		b.seriesBlockIterators,
		b.seriesMetas,
		b.meta,
		b.options,
	)
}

func iteratorBatchingFn(
	concurrency int,
	seriesBlockIterators []encoding.SeriesIterator,
	seriesMetas []block.SeriesMeta,
	meta block.Metadata,
	opts Options,
) ([]block.SeriesIterBatch, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("batch size %d must be greater than 0", concurrency)
	}

	var (
		iterCount  = len(seriesBlockIterators)
		iters      = make([]block.SeriesIterBatch, 0, concurrency)
		chunkSize  = iterCount / concurrency
		remainder  = iterCount % concurrency
		chunkSizes = make([]int, concurrency)
	)

	util.MemsetInt(chunkSizes, chunkSize)
	for i := 0; i < remainder; i++ {
		chunkSizes[i] = chunkSizes[i] + 1
	}

	start := 0
	for _, chunkSize := range chunkSizes {
		end := start + chunkSize

		if end > iterCount {
			end = iterCount
		}

		iter := NewEncodedSeriesIter(
			meta, seriesMetas[start:end], seriesBlockIterators[start:end],
			opts.Instrumented(),
		)

		iters = append(iters, block.SeriesIterBatch{
			Iter: iter,
			Size: end - start,
		})

		start = end
	}

	return iters, nil
}

// BlockSeriesProcessor processes blocks.
type BlockSeriesProcessor interface {
	Process(bl block.Block, opts Options, fn BlockSeriesProcessorFn) error
}

// BlockSeriesProcessorFn processes an individual series iterator.
type BlockSeriesProcessorFn func(
	iter block.SeriesIter,
) error

// NewBlockSeriesProcessor creates a standard block processor.
func NewBlockSeriesProcessor() BlockSeriesProcessor {
	return seriesBlockProcessor{}
}

type seriesBlockProcessor struct {
}

func (p seriesBlockProcessor) Process(
	bl block.Block,
	opts Options,
	fn BlockSeriesProcessorFn,
) error {
	iter, err := bl.SeriesIter()
	if err != nil {
		return err
	}
	return fn(iter)
}
