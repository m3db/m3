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

package pools

import (
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"
)

const (
	// TODO: add capabilities to get this from configs
	defaultReplicas                    = 3
	defaultSeriesIteratorPoolSize      = 2 << 12 // ~8k
	defaultCheckedBytesWrapperPoolSize = 2 << 12 // ~8k
	defaultPoolableConcurrentQueries   = 64
	defaultPoolableSeriesPerQuery      = 4096
)

var (
	defaultSeriesIteratorsPoolBuckets = []pool.Bucket{
		{
			Capacity: defaultPoolableSeriesPerQuery,
			Count:    defaultPoolableConcurrentQueries,
		},
	}
	defaultSeriesIDBytesPoolBuckets = []pool.Bucket{
		{
			Capacity: 256, // Can pool IDs up to 256 in size with this bucket.
			Count:    defaultPoolableSeriesPerQuery,
		},
		{
			Capacity: 1024, // Can pool IDs up to 1024 in size with this bucket.
			Count:    defaultPoolableSeriesPerQuery,
		},
	}
)

// BuildWorkerPools builds a worker pool
func BuildWorkerPools(
	instrumentOptions instrument.Options,
	readPoolPolicy, writePoolPolicy xconfig.WorkerPoolPolicy,
	scope tally.Scope,
) (xsync.PooledWorkerPool, xsync.PooledWorkerPool, error) {
	opts, readPoolSize := readPoolPolicy.Options()
	opts = opts.SetInstrumentOptions(instrumentOptions.
		SetMetricsScope(scope.SubScope("read-worker-pool")))
	readWorkerPool, err := xsync.NewPooledWorkerPool(readPoolSize, opts)
	if err != nil {
		return nil, nil, err
	}

	readWorkerPool.Init()
	opts, writePoolSize := writePoolPolicy.Options()
	opts = opts.SetInstrumentOptions(instrumentOptions.
		SetMetricsScope(scope.SubScope("write-worker-pool")))
	writeWorkerPool, err := xsync.NewPooledWorkerPool(writePoolSize, opts)
	if err != nil {
		return nil, nil, err
	}

	writeWorkerPool.Init()
	return readWorkerPool, writeWorkerPool, nil
}

type sessionPools struct {
	multiReaderIteratorArray encoding.MultiReaderIteratorArrayPool
	multiReaderIterator      encoding.MultiReaderIteratorPool
	seriesIterators          encoding.MutableSeriesIteratorsPool
	seriesIterator           encoding.SeriesIteratorPool
	checkedBytesWrapper      xpool.CheckedBytesWrapperPool
	id                       ident.Pool
	tagEncoder               serialize.TagEncoderPool
	tagDecoder               serialize.TagDecoderPool
}

func (s sessionPools) MultiReaderIteratorArray() encoding.MultiReaderIteratorArrayPool {
	return s.multiReaderIteratorArray
}

func (s sessionPools) MultiReaderIterator() encoding.MultiReaderIteratorPool {
	return s.multiReaderIterator
}

func (s sessionPools) MutableSeriesIterators() encoding.MutableSeriesIteratorsPool {
	return s.seriesIterators
}

func (s sessionPools) SeriesIterator() encoding.SeriesIteratorPool {
	return s.seriesIterator
}

func (s sessionPools) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool {
	return s.checkedBytesWrapper
}

func (s sessionPools) ID() ident.Pool {
	return s.id
}

func (s sessionPools) TagEncoder() serialize.TagEncoderPool {
	return s.tagEncoder
}

func (s sessionPools) TagDecoder() serialize.TagDecoderPool {
	return s.tagDecoder
}

// BuildIteratorPoolsOptions is a set of build iterator pools.
type BuildIteratorPoolsOptions struct {
	Replicas                    int
	SeriesIteratorPoolSize      int
	SeriesIteratorsPoolBuckets  []pool.Bucket
	SeriesIDBytesPoolBuckets    []pool.Bucket
	CheckedBytesWrapperPoolSize int
}

// ReplicasOrDefault returns the replicas or default.
func (o BuildIteratorPoolsOptions) ReplicasOrDefault() int {
	if o.Replicas <= 0 {
		return defaultReplicas
	}
	return o.Replicas
}

// SeriesIteratorPoolSizeOrDefault returns the replicas or default.
func (o BuildIteratorPoolsOptions) SeriesIteratorPoolSizeOrDefault() int {
	if o.SeriesIteratorPoolSize <= 0 {
		return defaultSeriesIteratorPoolSize
	}
	return o.SeriesIteratorPoolSize
}

// CheckedBytesWrapperPoolSizeOrDefault returns the checked bytes
// wrapper pool size or default.
func (o BuildIteratorPoolsOptions) CheckedBytesWrapperPoolSizeOrDefault() int {
	if o.CheckedBytesWrapperPoolSize <= 0 {
		return defaultCheckedBytesWrapperPoolSize
	}
	return o.CheckedBytesWrapperPoolSize
}

// SeriesIteratorsPoolBucketsOrDefault returns the series iterator pool
// buckets or defaults.
func (o BuildIteratorPoolsOptions) SeriesIteratorsPoolBucketsOrDefault() []pool.Bucket {
	if len(o.SeriesIteratorsPoolBuckets) == 0 {
		return defaultSeriesIteratorsPoolBuckets
	}
	return o.SeriesIteratorsPoolBuckets
}

// SeriesIDBytesPoolBucketsOrDefault returns the bytes pool buckets or defaults.
func (o BuildIteratorPoolsOptions) SeriesIDBytesPoolBucketsOrDefault() []pool.Bucket {
	if len(o.SeriesIDBytesPoolBuckets) == 0 {
		return defaultSeriesIDBytesPoolBuckets
	}
	return o.SeriesIDBytesPoolBuckets
}

// BuildIteratorPools build iterator pools if they are unavailable from
// m3db (e.g. if running standalone query)
func BuildIteratorPools(
	encodingOpts encoding.Options,
	opts BuildIteratorPoolsOptions,
) encoding.IteratorPools {
	// TODO: add instrumentation options to these pools
	pools := sessionPools{}

	defaultPerSeriesIteratorsBuckets := opts.SeriesIteratorsPoolBucketsOrDefault()

	pools.multiReaderIteratorArray = encoding.NewMultiReaderIteratorArrayPool(defaultPerSeriesIteratorsBuckets)
	pools.multiReaderIteratorArray.Init()

	defaultPerSeriesPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.SeriesIteratorPoolSizeOrDefault())

	readerIteratorPoolPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.SeriesIteratorPoolSizeOrDefault() * opts.ReplicasOrDefault())

	readerIteratorPool := encoding.NewReaderIteratorPool(readerIteratorPoolPoolOpts)

	encodingOpts = encodingOpts.
		SetReaderIteratorPool(readerIteratorPool)

	readerIteratorPool.Init(m3tsz.DefaultReaderIteratorAllocFn(encodingOpts))

	pools.multiReaderIterator = encoding.NewMultiReaderIteratorPool(defaultPerSeriesPoolOpts)
	pools.multiReaderIterator.Init(
		func(r xio.Reader64, s namespace.SchemaDescr) encoding.ReaderIterator {
			iter := readerIteratorPool.Get()
			iter.Reset(r, s)
			return iter
		})

	pools.seriesIterator = encoding.NewSeriesIteratorPool(defaultPerSeriesPoolOpts)
	pools.seriesIterator.Init()

	pools.seriesIterators = encoding.NewMutableSeriesIteratorsPool(defaultPerSeriesIteratorsBuckets)
	pools.seriesIterators.Init()

	wrapperPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.CheckedBytesWrapperPoolSizeOrDefault())
	pools.checkedBytesWrapper = xpool.NewCheckedBytesWrapperPool(wrapperPoolOpts)
	pools.checkedBytesWrapper.Init()

	pools.tagEncoder = serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		defaultPerSeriesPoolOpts)
	pools.tagEncoder.Init()

	tagDecoderCheckBytesWrapperPoolSize := 0
	tagDecoderOpts := serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
		// We pass in a preallocated pool so use a zero sized pool in options init.
		CheckBytesWrapperPoolSize: &tagDecoderCheckBytesWrapperPoolSize,
	})
	tagDecoderOpts = tagDecoderOpts.SetCheckedBytesWrapperPool(pools.checkedBytesWrapper)

	pools.tagDecoder = serialize.NewTagDecoderPool(tagDecoderOpts, defaultPerSeriesPoolOpts)
	pools.tagDecoder.Init()

	bytesPool := pool.NewCheckedBytesPool(opts.SeriesIDBytesPoolBucketsOrDefault(),
		nil, func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})
	bytesPool.Init()

	pools.id = ident.NewPool(bytesPool, ident.PoolOptions{
		IDPoolOptions:           defaultPerSeriesPoolOpts,
		TagsPoolOptions:         defaultPerSeriesPoolOpts,
		TagsIteratorPoolOptions: defaultPerSeriesPoolOpts,
	})

	return pools
}
