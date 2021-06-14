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

package m3db

import (
	"context"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	queryconsolidator "github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
)

// Options describes the options for encoded block converters.
// These options are generally config-backed and don't usually change across
// queries, unless certain query string parameters are present.
type Options interface {
	// SetSplitSeriesByBlock determines if the converter will split the series
	// by blocks, or if it will instead treat the entire series as a single block.
	SetSplitSeriesByBlock(bool) Options
	// SplittingSeriesByBlock returns true iff lookback duration is 0, and the
	// options has not been forced to return a single block.
	SplittingSeriesByBlock() bool
	// SetLookbackDuration sets the lookback duration.
	SetLookbackDuration(time.Duration) Options
	// LookbackDuration returns the lookback duration.
	LookbackDuration() time.Duration
	// SetLookbackDuration sets the consolidation function for the converter.
	SetConsolidationFunc(consolidators.ConsolidationFunc) Options
	// LookbackDuration returns the consolidation function.
	ConsolidationFunc() consolidators.ConsolidationFunc
	// SetTagOptions sets the tag options.
	SetTagOptions(models.TagOptions) Options
	// TagOptions returns the tag options.
	TagOptions() models.TagOptions
	// SetIterAlloc sets the iterator allocator.
	SetIterAlloc(encoding.ReaderIteratorAllocate) Options
	// IterAlloc returns the reader iterator allocator.
	IterAlloc() encoding.ReaderIteratorAllocate
	// SetIteratorPools sets the iterator pools for the converter.
	SetIteratorPools(encoding.IteratorPools) Options
	// IteratorPools returns the iterator pools for the converter.
	IteratorPools() encoding.IteratorPools
	// SetCheckedBytesPool sets the checked bytes pool for the converter.
	SetCheckedBytesPool(pool.CheckedBytesPool) Options
	// CheckedBytesPool returns the checked bytes pools for the converter.
	CheckedBytesPool() pool.CheckedBytesPool
	// SetReadWorkerPool sets the read worker pool for the converter.
	SetReadWorkerPool(xsync.PooledWorkerPool) Options
	// ReadWorkerPool returns the read worker pool for the converter.
	ReadWorkerPool() xsync.PooledWorkerPool
	// SetReadWorkerPool sets the write worker pool for the converter.
	SetWriteWorkerPool(xsync.PooledWorkerPool) Options
	// ReadWorkerPool returns the write worker pool for the converter.
	WriteWorkerPool() xsync.PooledWorkerPool
	// SetSeriesConsolidationMatchOptions sets series consolidation options.
	SetSeriesConsolidationMatchOptions(value queryconsolidator.MatchOptions) Options
	// SetSeriesConsolidationMatchOptions sets series consolidation options.
	SeriesConsolidationMatchOptions() queryconsolidator.MatchOptions
	// SetSeriesIteratorProcessor sets the series iterator processor.
	SetSeriesIteratorProcessor(SeriesIteratorProcessor) Options
	// SeriesIteratorProcessor returns the series iterator processor.
	SeriesIteratorProcessor() SeriesIteratorProcessor
	// SetIteratorBatchingFn sets the batching function for the converter.
	SetIteratorBatchingFn(IteratorBatchingFn) Options
	// IteratorBatchingFn returns the batching function for the converter.
	IteratorBatchingFn() IteratorBatchingFn
	// SetBlockSeriesProcessor set the block series processor.
	SetBlockSeriesProcessor(value BlockSeriesProcessor) Options
	// BlockSeriesProcessor returns the block series processor.
	BlockSeriesProcessor() BlockSeriesProcessor
	// SetCustomAdminOptions sets custom admin options.
	SetCustomAdminOptions([]client.CustomAdminOption) Options
	// CustomAdminOptions gets custom admin options.
	CustomAdminOptions() []client.CustomAdminOption
	// SetInstrumented marks if the encoding step should have instrumentation enabled.
	SetInstrumented(bool) Options
	// Instrumented returns if the encoding step should have instrumentation enabled.
	Instrumented() bool
	// Validate ensures that the given block options are valid.
	Validate() error
}

// SeriesIteratorProcessor optionally defines methods to process series iterators.
type SeriesIteratorProcessor interface {
	// InspectSeries inspects SeriesIterator slices for a given query.
	InspectSeries(
		ctx context.Context,
		query index.Query,
		queryOpts index.QueryOptions,
		seriesIterators []encoding.SeriesIterator,
	) error
}

// IteratorBatchingFn determines how the iterator is split into batches.
type IteratorBatchingFn func(
	concurrency int,
	seriesBlockIterators []encoding.SeriesIterator,
	seriesMetas []block.SeriesMeta,
	meta block.Metadata,
	opts Options,
) ([]block.SeriesIterBatch, error)

// GraphiteBlockIteratorsFn returns block iterators for graphite decoding.
type GraphiteBlockIteratorsFn func(
	block.Block,
) ([]block.SeriesIterBatch, error)

type peekValue struct {
	started  bool
	finished bool
	point    ts.Datapoint
}
