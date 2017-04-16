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

// ObjectPoolConfiguration contains configuration for object pools.
type ObjectPoolConfiguration struct {
	// The size of the pool.
	Size int `yaml:"size"`

	// The watermark configuration.
	WaterMark WaterMarkConfiguration `yaml:"waterMark"`
}

// CapacityPoolConfiguration contains configuration for pools containing objects
// with capacity attributes (e.g., slices).
type CapacityPoolConfiguration struct {
	// The size of the pool.
	Size int `yaml:"size"`

	// The capacity of items in the pool.
	Capacity int `yaml:"capacity"`

	// The watermark configuration.
	WaterMark WaterMarkConfiguration `yaml:"waterMark"`
}

// BucketizedPoolConfiguration contains configuration for bucketized pools.
type BucketizedPoolConfiguration struct {
	// The pool bucket configuration.
	Buckets []BucketConfiguration `yaml:"buckets"`

	// The watermark configuration.
	WaterMark WaterMarkConfiguration `yaml:"waterMark"`
}

// BucketConfiguration contains configuration for a pool bucket.
type BucketConfiguration struct {
	// The count of the items in the bucket.
	Count int `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// WaterMarkConfiguration contains watermark configuration for pools.
type WaterMarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}
