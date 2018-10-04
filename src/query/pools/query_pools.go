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
	"io"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

const (
	// TODO: add capabilities to get this from configs
	replicas                    = 1
	iteratorPoolSize            = 65536
	checkedBytesWrapperPoolSize = 65536
	defaultIdentifierPoolSize   = 8192
	defaultBucketCapacity       = 256
)

// BuildWorkerPools builds a worker pool
func BuildWorkerPools(
	instrumentOptions instrument.Options,
	scope tally.Scope,
	readPoolPolicy, writePoolPolicy xconfig.WorkerPoolPolicy,
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

func buildBuckets() []pool.Bucket {
	return []pool.Bucket{
		{Capacity: defaultBucketCapacity, Count: defaultIdentifierPoolSize},
	}
}

// BuildIteratorPools build iterator pools if they are unavailable from
// m3db (e.g. if running standalone query)
func BuildIteratorPools() encoding.IteratorPools {
	// TODO: add instrumentation options to these pools
	pools := sessionPools{}
	pools.multiReaderIteratorArray = encoding.NewMultiReaderIteratorArrayPool([]pool.Bucket{
		pool.Bucket{
			Capacity: replicas,
			Count:    iteratorPoolSize,
		},
	})
	pools.multiReaderIteratorArray.Init()

	size := replicas * iteratorPoolSize
	poolOpts := pool.NewObjectPoolOptions().
		SetSize(size)
	pools.multiReaderIterator = encoding.NewMultiReaderIteratorPool(poolOpts)
	encodingOpts := encoding.NewOptions()
	readerIterAlloc := func(r io.Reader) encoding.ReaderIterator {
		intOptimized := m3tsz.DefaultIntOptimizationEnabled
		return m3tsz.NewReaderIterator(r, intOptimized, encodingOpts)
	}

	pools.multiReaderIterator.Init(readerIterAlloc)

	seriesIteratorPoolOpts := pool.NewObjectPoolOptions().
		SetSize(iteratorPoolSize)
	pools.seriesIterator = encoding.NewSeriesIteratorPool(seriesIteratorPoolOpts)
	pools.seriesIterator.Init()

	pools.seriesIterators = encoding.NewMutableSeriesIteratorsPool(buildBuckets())
	pools.seriesIterators.Init()

	wrapperPoolOpts := pool.NewObjectPoolOptions().
		SetSize(checkedBytesWrapperPoolSize)
	pools.checkedBytesWrapper = xpool.NewCheckedBytesWrapperPool(wrapperPoolOpts)
	pools.checkedBytesWrapper.Init()

	pools.tagEncoder = serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions(),
	)
	pools.tagEncoder.Init()

	pools.tagDecoder = serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(),
		pool.NewObjectPoolOptions(),
	)
	pools.tagDecoder.Init()

	bytesPool := pool.NewCheckedBytesPool(buildBuckets(), nil,
		func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})
	bytesPool.Init()

	idPoolOpts := pool.NewObjectPoolOptions().
		SetSize(defaultIdentifierPoolSize)

	pools.id = ident.NewPool(bytesPool, ident.PoolOptions{
		IDPoolOptions:           idPoolOpts,
		TagsPoolOptions:         idPoolOpts,
		TagsIteratorPoolOptions: idPoolOpts,
	})

	return pools
}
