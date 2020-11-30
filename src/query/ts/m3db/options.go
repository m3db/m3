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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	queryconsolidator "github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
)

var (
	defaultCapacity         = 1024
	defaultCount            = 10
	defaultLookbackDuration = time.Duration(0)
	defaultConsolidationFn  = consolidators.TakeLast
	defaultIterAlloc        = func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
	defaultIteratorBatchingFn   = iteratorBatchingFn
	defaultBlockSeriesProcessor = NewBlockSeriesProcessor()
	defaultInstrumented         = true
)

type encodedBlockOptions struct {
	splitSeries                   bool
	lookbackDuration              time.Duration
	consolidationFn               consolidators.ConsolidationFunc
	tagOptions                    models.TagOptions
	iterAlloc                     encoding.ReaderIteratorAllocate
	pools                         encoding.IteratorPools
	checkedPools                  pool.CheckedBytesPool
	readWorkerPools               xsync.PooledWorkerPool
	writeWorkerPools              xsync.PooledWorkerPool
	queryConsolidatorMatchOptions queryconsolidator.MatchOptions
	seriesIteratorProcessor       SeriesIteratorProcessor
	batchingFn                    IteratorBatchingFn
	blockSeriesProcessor          BlockSeriesProcessor
	adminOptions                  []client.CustomAdminOption
	instrumented                  bool
}

// NewOptions creates a default encoded block options which dictates how
// encoded blocks are generated.
func NewOptions() Options {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{{
		Capacity: defaultCapacity,
		Count:    defaultCount,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()

	iteratorPools := pools.BuildIteratorPools(pools.BuildIteratorPoolsOptions{})
	return newOptions(bytesPool, iteratorPools)
}

func newOptions(
	bytesPool pool.CheckedBytesPool,
	iteratorPools encoding.IteratorPools,
) Options {
	return &encodedBlockOptions{
		lookbackDuration:     defaultLookbackDuration,
		consolidationFn:      defaultConsolidationFn,
		tagOptions:           models.NewTagOptions(),
		iterAlloc:            defaultIterAlloc,
		pools:                iteratorPools,
		checkedPools:         bytesPool,
		batchingFn:           defaultIteratorBatchingFn,
		blockSeriesProcessor: defaultBlockSeriesProcessor,
		instrumented:         defaultInstrumented,
		queryConsolidatorMatchOptions: queryconsolidator.MatchOptions{
			MatchType: queryconsolidator.MatchIDs,
		},
	}
}

func (o *encodedBlockOptions) SetSplitSeriesByBlock(split bool) Options {
	opts := *o
	opts.splitSeries = split
	return &opts
}

func (o *encodedBlockOptions) SplittingSeriesByBlock() bool {
	return o.splitSeries
}

func (o *encodedBlockOptions) SetLookbackDuration(lookback time.Duration) Options {
	opts := *o
	opts.lookbackDuration = lookback
	return &opts
}

func (o *encodedBlockOptions) LookbackDuration() time.Duration {
	return o.lookbackDuration
}

func (o *encodedBlockOptions) SetConsolidationFunc(fn consolidators.ConsolidationFunc) Options {
	opts := *o
	opts.consolidationFn = fn
	return &opts
}

func (o *encodedBlockOptions) ConsolidationFunc() consolidators.ConsolidationFunc {
	return o.consolidationFn
}

func (o *encodedBlockOptions) SetTagOptions(tagOptions models.TagOptions) Options {
	opts := *o
	opts.tagOptions = tagOptions
	return &opts
}

func (o *encodedBlockOptions) TagOptions() models.TagOptions {
	return o.tagOptions
}

func (o *encodedBlockOptions) SetIterAlloc(ia encoding.ReaderIteratorAllocate) Options {
	opts := *o
	opts.iterAlloc = ia
	return &opts
}

func (o *encodedBlockOptions) IterAlloc() encoding.ReaderIteratorAllocate {
	return o.iterAlloc
}

func (o *encodedBlockOptions) SetIteratorPools(p encoding.IteratorPools) Options {
	opts := *o
	opts.pools = p
	return &opts
}

func (o *encodedBlockOptions) IteratorPools() encoding.IteratorPools {
	return o.pools
}

func (o *encodedBlockOptions) SetCheckedBytesPool(p pool.CheckedBytesPool) Options {
	opts := *o
	opts.checkedPools = p
	return &opts
}

func (o *encodedBlockOptions) CheckedBytesPool() pool.CheckedBytesPool {
	return o.checkedPools
}

func (o *encodedBlockOptions) SetReadWorkerPool(p xsync.PooledWorkerPool) Options {
	opts := *o
	opts.readWorkerPools = p
	return &opts
}

func (o *encodedBlockOptions) ReadWorkerPool() xsync.PooledWorkerPool {
	return o.readWorkerPools
}

func (o *encodedBlockOptions) SetWriteWorkerPool(p xsync.PooledWorkerPool) Options {
	opts := *o
	opts.writeWorkerPools = p
	return &opts
}

func (o *encodedBlockOptions) WriteWorkerPool() xsync.PooledWorkerPool {
	return o.writeWorkerPools
}

func (o *encodedBlockOptions) SetSeriesConsolidationMatchOptions(
	value queryconsolidator.MatchOptions) Options {
	opts := *o
	opts.queryConsolidatorMatchOptions = value
	return &opts
}

func (o *encodedBlockOptions) SeriesConsolidationMatchOptions() queryconsolidator.MatchOptions {
	return o.queryConsolidatorMatchOptions
}

func (o *encodedBlockOptions) SetSeriesIteratorProcessor(p SeriesIteratorProcessor) Options {
	opts := *o
	opts.seriesIteratorProcessor = p
	return &opts
}

func (o *encodedBlockOptions) SeriesIteratorProcessor() SeriesIteratorProcessor {
	return o.seriesIteratorProcessor
}

func (o *encodedBlockOptions) SetIteratorBatchingFn(fn IteratorBatchingFn) Options {
	opts := *o
	opts.batchingFn = fn
	return &opts
}

func (o *encodedBlockOptions) IteratorBatchingFn() IteratorBatchingFn {
	return o.batchingFn
}

func (o *encodedBlockOptions) SetBlockSeriesProcessor(fn BlockSeriesProcessor) Options {
	opts := *o
	opts.blockSeriesProcessor = fn
	return &opts
}

func (o *encodedBlockOptions) BlockSeriesProcessor() BlockSeriesProcessor {
	return o.blockSeriesProcessor
}

func (o *encodedBlockOptions) SetCustomAdminOptions(
	val []client.CustomAdminOption) Options {
	opts := *o
	opts.adminOptions = val
	return &opts

}

func (o *encodedBlockOptions) CustomAdminOptions() []client.CustomAdminOption {
	return o.adminOptions
}

func (o *encodedBlockOptions) SetInstrumented(i bool) Options {
	opts := *o
	opts.instrumented = i
	return &opts
}

func (o *encodedBlockOptions) Instrumented() bool {
	return o.instrumented
}

func (o *encodedBlockOptions) Validate() error {
	if o.lookbackDuration < 0 {
		return errors.New("unable to validate block options; negative lookback")
	}

	if err := o.tagOptions.Validate(); err != nil {
		return fmt.Errorf("unable to validate tag options, err: %v", err)
	}

	return nil
}
