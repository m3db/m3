// Copyright (c) 2017 Uber Technologies, Inc.
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

// ObjectPoolConfig contains configuration for object pools.
type ObjectPoolConfig struct {
	// The size of the pool.
	Size int `yaml:"size"`

	// The watermark setting.
	WaterMark WaterMarkConfig `yaml:"waterMark"`
}

// CapacityPoolConfig contains configuration for pools containing objects
// with capacity attributes (e.g., slices).
type CapacityPoolConfig struct {
	// The size of the pool.
	Size int `yaml:"size"`

	// The capacity of items in the pool.
	Capacity int `yaml:"capacity"`

	// The watermark setting.
	WaterMark WaterMarkConfig `yaml:"waterMark"`
}

// BucketizedPoolConfig contains configuration for bucketized pools.
type BucketizedPoolConfig struct {
	// The pool bucket configuration.
	Buckets []BucketConfig `yaml:"buckets"`

	// The watermark setting.
	WaterMark WaterMarkConfig `yaml:"waterMark"`
}

// BucketConfig contains configuration for a pool bucket.
type BucketConfig struct {
	// The count of the items in the bucket.
	Count int `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// WaterMarkConfig contains watermark configuration for pools.
type WaterMarkConfig struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}
