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

package pool

// PoolAllocator allocates an object for a pool.
type PoolAllocator func() interface{}

// Work is a unit of item to be worked on.
type Work func()

// ObjectPool provides a pool for objects
type ObjectPool interface {
	// Init initializes the pool.
	Init(alloc PoolAllocator)

	// Get provides an object from the pool
	Get() interface{}

	// Put returns an object to the pool
	Put(obj interface{})
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

// BytesPool provides a pool for variable size buffers
type BytesPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a buffer from the pool
	Get(capacity int) []byte

	// Put returns a buffer to the pool
	Put(buffer []byte)
}

// WorkerPool provides a pool for goroutines.
type WorkerPool interface {
	// Init initializes the pool.
	Init()

	// Go waits until the next worker becomes available and executes it.
	Go(work Work)

	// GoIfAvailable performs the work inside a worker if one is available and returns true,
	// or false otherwise.
	GoIfAvailable(work Work) bool
}
