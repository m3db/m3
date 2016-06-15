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

package memtsdb

// PoolAllocator allocates an object for a pool.
type PoolAllocator func() interface{}

// EncoderAllocate allocates an encoder for a pool.
type EncoderAllocate func() Encoder

// IteratorAllocate allocates an iterator for a pool.
type IteratorAllocate func() Iterator

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

// IteratorPool provides a pool for iterators
type IteratorPool interface {
	// Init initializes the pool.
	Init(alloc IteratorAllocate)

	// Get provides an iterator from the pool
	Get() Iterator

	// Put returns an iterator to the pool
	Put(iter Iterator)
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
