// Copyright (c) 2016 Uber Technologies, Inc.
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

package fs

import (
	"errors"
	"runtime"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
)

var (
	// Allow max concurrency to match available CPUs.
	defaultFetchConcurrency = runtime.NumCPU()
	defaultCacheOnRetrieve  = false

	errBlockLeaseManagerNotSet = errors.New("block lease manager is not set")
)

type blockRetrieverOptions struct {
	requestPool       RetrieveRequestPool
	bytesPool         pool.CheckedBytesPool
	fetchConcurrency  int
	cacheOnRetrieve   bool
	identifierPool    ident.Pool
	blockLeaseManager block.LeaseManager
	queryLimits       limits.QueryLimits
}

// NewBlockRetrieverOptions creates a new set of block retriever options
func NewBlockRetrieverOptions() BlockRetrieverOptions {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		{Count: 4096, Capacity: 128},
	}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()

	segmentReaderPool := xio.NewSegmentReaderPool(nil)
	segmentReaderPool.Init()

	requestPool := NewRetrieveRequestPool(segmentReaderPool, nil)
	requestPool.Init()

	o := &blockRetrieverOptions{
		requestPool:      requestPool,
		bytesPool:        bytesPool,
		fetchConcurrency: defaultFetchConcurrency,
		cacheOnRetrieve:  defaultCacheOnRetrieve,
		identifierPool:   ident.NewPool(bytesPool, ident.PoolOptions{}),
		queryLimits:      limits.NoOpQueryLimits(),
	}

	return o
}

func (o *blockRetrieverOptions) Validate() error {
	if o.blockLeaseManager == nil {
		return errBlockLeaseManagerNotSet
	}
	return nil
}

func (o *blockRetrieverOptions) SetRetrieveRequestPool(value RetrieveRequestPool) BlockRetrieverOptions {
	opts := *o
	opts.requestPool = value
	return &opts
}

func (o *blockRetrieverOptions) RetrieveRequestPool() RetrieveRequestPool {
	return o.requestPool
}

func (o *blockRetrieverOptions) SetBytesPool(value pool.CheckedBytesPool) BlockRetrieverOptions {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *blockRetrieverOptions) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *blockRetrieverOptions) SetFetchConcurrency(value int) BlockRetrieverOptions {
	opts := *o
	opts.fetchConcurrency = value
	return &opts
}

func (o *blockRetrieverOptions) FetchConcurrency() int {
	return o.fetchConcurrency
}

func (o *blockRetrieverOptions) SetCacheBlocksOnRetrieve(value bool) BlockRetrieverOptions {
	opts := *o
	opts.cacheOnRetrieve = value
	return &opts
}

func (o *blockRetrieverOptions) CacheBlocksOnRetrieve() bool {
	return o.cacheOnRetrieve
}

func (o *blockRetrieverOptions) SetIdentifierPool(value ident.Pool) BlockRetrieverOptions {
	opts := *o
	opts.identifierPool = value
	return &opts
}

func (o *blockRetrieverOptions) IdentifierPool() ident.Pool {
	return o.identifierPool
}

func (o *blockRetrieverOptions) SetBlockLeaseManager(leaseMgr block.LeaseManager) BlockRetrieverOptions {
	opts := *o
	opts.blockLeaseManager = leaseMgr
	return &opts
}

func (o *blockRetrieverOptions) BlockLeaseManager() block.LeaseManager {
	return o.blockLeaseManager
}

func (o *blockRetrieverOptions) SetQueryLimits(value limits.QueryLimits) BlockRetrieverOptions {
	opts := *o
	opts.queryLimits = value
	return &opts
}

func (o *blockRetrieverOptions) QueryLimits() limits.QueryLimits {
	return o.queryLimits
}
