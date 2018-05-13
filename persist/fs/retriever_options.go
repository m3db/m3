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
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

const (
	defaultRequestPoolSize  = 16384
	defaultFetchConcurrency = 2
)

type blockRetrieverOptions struct {
	requestPoolOpts   pool.ObjectPoolOptions
	bytesPool         pool.CheckedBytesPool
	segmentReaderPool xio.SegmentReaderPool
	fetchConcurrency  int
	identifierPool    ident.Pool
}

// NewBlockRetrieverOptions creates a new set of block retriever options
func NewBlockRetrieverOptions() BlockRetrieverOptions {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		pool.Bucket{Count: 4096, Capacity: 128},
	}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	o := &blockRetrieverOptions{
		requestPoolOpts: pool.NewObjectPoolOptions().
			SetSize(defaultRequestPoolSize),
		bytesPool:         bytesPool,
		segmentReaderPool: xio.NewSegmentReaderPool(nil),
		fetchConcurrency:  defaultFetchConcurrency,
		identifierPool:    ident.NewPool(bytesPool, ident.PoolOptions{}),
	}
	o.segmentReaderPool.Init()
	return o
}

func (o *blockRetrieverOptions) SetRequestPoolOptions(value pool.ObjectPoolOptions) BlockRetrieverOptions {
	opts := *o
	opts.requestPoolOpts = value
	return &opts
}

func (o *blockRetrieverOptions) RequestPoolOptions() pool.ObjectPoolOptions {
	return o.requestPoolOpts
}

func (o *blockRetrieverOptions) SetBytesPool(value pool.CheckedBytesPool) BlockRetrieverOptions {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *blockRetrieverOptions) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *blockRetrieverOptions) SetSegmentReaderPool(value xio.SegmentReaderPool) BlockRetrieverOptions {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *blockRetrieverOptions) SegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *blockRetrieverOptions) SetFetchConcurrency(value int) BlockRetrieverOptions {
	opts := *o
	opts.fetchConcurrency = value
	return &opts
}

func (o *blockRetrieverOptions) FetchConcurrency() int {
	return o.fetchConcurrency
}

func (o *blockRetrieverOptions) SetIdentifierPool(value ident.Pool) BlockRetrieverOptions {
	opts := *o
	opts.identifierPool = value
	return &opts
}

func (o *blockRetrieverOptions) IdentifierPool() ident.Pool {
	return o.identifierPool
}
