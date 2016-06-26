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

package m3db

// PoolAllocator allocates an object for a pool.
type PoolAllocator func() interface{}

// EncoderAllocate allocates an encoder for a pool.
type EncoderAllocate func() Encoder

// SingleReaderIteratorAllocate allocates a SingleReaderIterator for a pool.
type SingleReaderIteratorAllocate func() SingleReaderIterator

// MultiReaderIteratorAllocate allocates a MultiReaderIterator for a pool.
type MultiReaderIteratorAllocate func() MultiReaderIterator

type MixedReadersIteratorPoolAllocate func() MixedReadersIterator

type MixedReadersIteratorArrayPoolAllocate func() []MixedReadersIterator

type SeriesIteratorPoolAllocate func() SeriesIterator

type SeriesIteratorPoolArrayAllocate func() []SeriesIterator

// ObjectPool provides a pool for objects
type ObjectPool interface {
	// Init initializes the pool.
	Init(alloc PoolAllocator)

	// Get provides an object from the pool
	Get() interface{}

	// Put returns an object to the pool
	Put(obj interface{})
}

// BytesPool provides a pool for variable size buffers
type BytesPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a buffer from the pool
	Get(capacity int) []byte

	// Put returns a buffer to the pool
	Put(buffer []byte)
}

// ContextPool provides a pool for contexts
type ContextPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a context from the pool
	Get() Context

	// Put returns a context to the pool
	Put(ctx Context)
}

// EncoderPool provides a pool for encoders
type EncoderPool interface {
	// Init initializes the pool.
	Init(alloc EncoderAllocate)

	// Get provides an encoder from the pool
	Get() Encoder

	// Put returns an encoder to the pool
	Put(encoder Encoder)
}

// SegmentReaderPool provides a pool for segment readers
type SegmentReaderPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a segment reader from the pool
	Get() SegmentReader

	// Put returns a segment reader to the pool
	Put(reader SegmentReader)
}

// SingleReaderIteratorPool provides a pool for SingleReaderIterators
type SingleReaderIteratorPool interface {
	// Init initializes the pool.
	Init(alloc SingleReaderIteratorAllocate)

	// Get provides a SingleReaderIterator from the pool
	Get() SingleReaderIterator

	// Put returns a SingleReaderIterator to the pool
	Put(iter SingleReaderIterator)
}

// MultiReaderIteratorPool provides a pool for MultiReaderIterators
type MultiReaderIteratorPool interface {
	// Init initializes the pool.
	Init(alloc MultiReaderIteratorAllocate)

	// Get provides a MultiReaderIterator from the pool
	Get() MultiReaderIterator

	// Put returns a MultiReaderIterator to the pool
	Put(iter MultiReaderIterator)
}

type MixedReadersIteratorPool interface {
	Init(alloc MixedReadersIteratorPoolAllocate)
	Get() MixedReadersIterator
	Put(iters MixedReadersIterator)
}

type MixedReadersIteratorArrayPool interface {
	Init(alloc MixedReadersIteratorArrayPoolAllocate)
	Get(size int) []MixedReadersIterator
	Put(iters []MixedReadersIterator)
}

type SeriesIteratorPool interface {
	Init(alloc SeriesIteratorPoolAllocate)
	Get() SeriesIterator
	Put(iter SeriesIterator)
}

type SeriesArrayIteratorPool interface {
	Init(alloc SeriesIteratorPoolArrayAllocate)
	Get(size int) []SeriesIterator
	Put(iters []SeriesIterator)
}

// PoolBucket specifies a pool bucket
type PoolBucket struct {
	// Capacity is the size of each element in the bucket
	Capacity int

	// Count is the number of fixed elements in the bucket
	Count int
}

// PoolBucketByCapacity is a sortable collection of pool buckets
type PoolBucketByCapacity []PoolBucket

func (x PoolBucketByCapacity) Len() int {
	return len(x)
}

func (x PoolBucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x PoolBucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}
