// Copyright (c) 2022 Uber Technologies, Inc.
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

package test

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
)

var (
	buckets  = []pool.Bucket{{Capacity: 100, Count: 100}}
	poolOpts = pool.NewObjectPoolOptions().SetSize(1)
	mu       sync.Mutex
)

// MakeMockIteratorPool builds a mock iterator pool
func MakeMockIteratorPool() *MockIteratorPool {
	return &MockIteratorPool{}
}

// MockIteratorPool is an iterator pool used for testing
type MockIteratorPool struct {
	MriPoolUsed, SiPoolUsed, MriaPoolUsed,
	CbwPoolUsed, IdentPoolUsed, EncodePoolUsed, DecodePoolUsed bool
}

// MultiReaderIterator exposes the session's MultiReaderIteratorPool
func (ip *MockIteratorPool) MultiReaderIterator() encoding.MultiReaderIteratorPool {
	ip.MriPoolUsed = true
	mriPool := encoding.NewMultiReaderIteratorPool(nil)
	mriPool.Init(testIterAlloc)
	return mriPool
}

// SeriesIterator exposes the session's SeriesIteratorPool
func (ip *MockIteratorPool) SeriesIterator() encoding.SeriesIteratorPool {
	ip.SiPoolUsed = true
	siPool := encoding.NewSeriesIteratorPool(nil)
	siPool.Init()
	return siPool
}

// MultiReaderIteratorArray exposes the session's MultiReaderIteratorArrayPool
func (ip *MockIteratorPool) MultiReaderIteratorArray() encoding.MultiReaderIteratorArrayPool {
	ip.MriaPoolUsed = true
	mriaPool := encoding.NewMultiReaderIteratorArrayPool(nil)
	mriaPool.Init()
	return mriaPool
}

// CheckedBytesWrapper exposes the session's CheckedBytesWrapperPool
func (ip *MockIteratorPool) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool {
	ip.CbwPoolUsed = true
	cbwPool := xpool.NewCheckedBytesWrapperPool(nil)
	cbwPool.Init()
	return cbwPool
}

// ID exposes the session's identity pool
func (ip *MockIteratorPool) ID() ident.Pool {
	ip.IdentPoolUsed = true
	bytesPool := pool.NewCheckedBytesPool(buckets, nil,
		func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})
	bytesPool.Init()
	return ident.NewPool(bytesPool, ident.PoolOptions{})
}

// TagDecoder exposes the session's tag decoder pool
func (ip *MockIteratorPool) TagDecoder() serialize.TagDecoderPool {
	ip.DecodePoolUsed = true
	decoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		poolOpts)
	decoderPool.Init()
	return decoderPool
}

// TagEncoder exposes the session's tag encoder pool
func (ip *MockIteratorPool) TagEncoder() serialize.TagEncoderPool {
	mu.Lock()
	ip.EncodePoolUsed = true
	encoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), poolOpts)
	encoderPool.Init()
	mu.Unlock()
	return encoderPool
}
