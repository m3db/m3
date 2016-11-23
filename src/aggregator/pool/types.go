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

import (
	"github.com/uber-go/tally"
)

// Allocator allocates an object for a pool.
type Allocator func() interface{}

// ObjectPool provides a pool for objects
type ObjectPool interface {
	// Init initializes the pool.
	Init(alloc Allocator)

	// Get provides an object from the pool
	Get() interface{}

	// Put returns an object to the pool
	Put(obj interface{})
}

// ObjectPoolOptions provides options for an object pool
type ObjectPoolOptions interface {
	// SetSize sets the size of the object pool
	SetSize(value int) ObjectPoolOptions

	// Size returns the size of the object pool
	Size() int

	// SetMetricsScope sets the metrics scope
	SetMetricsScope(value tally.Scope) ObjectPoolOptions

	// MetricsScope returns the metrics scope
	MetricsScope() tally.Scope
}

// Bucket specifies a pool bucket
type Bucket struct {
	// Capacity is the size of each element in the bucket
	Capacity int

	// Count is the number of fixed elements in the bucket
	Count int
}

// BucketByCapacity is a sortable collection of pool buckets
type BucketByCapacity []Bucket

// Len returns the length of the buckets
func (x BucketByCapacity) Len() int {
	return len(x)
}

// Swap swaps the two buckets
func (x BucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

// Less returns true if the ith bucket is smaller than the jth bucket
func (x BucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}

// BucketizedAllocator allocates an object for a bucket given its capacity
type BucketizedAllocator func(capacity int) interface{}

// BucketizedObjectPool is a bucketized pool of objects
type BucketizedObjectPool interface {
	// Init initializes the pool
	Init(alloc BucketizedAllocator)

	// Get provides an object from the pool
	Get(capacity int) interface{}

	// Put returns an object to the pool
	Put(obj interface{}, capacity int)
}

// BucketizedObjectPoolOptions provides options for a bucketized pool
type BucketizedObjectPoolOptions interface {
	// SetBuckets sets the buckets of the pool
	SetBuckets(buckets []Bucket) BucketizedObjectPoolOptions

	// Buckets returns the buckets of the pool
	Buckets() []Bucket

	// SetMetricsScope sets the metrics scope
	SetMetricsScope(value tally.Scope) BucketizedObjectPoolOptions

	// MetricsScope returns the metrics scope
	MetricsScope() tally.Scope
}
