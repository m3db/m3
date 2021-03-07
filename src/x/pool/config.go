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

import (
	"errors"
	"strconv"

	"github.com/m3db/m3/src/x/instrument"
)

// ObjectPoolConfiguration contains configuration for object pools.
type ObjectPoolConfiguration struct {
	// The size of the pool.
	Size Size `yaml:"size"`

	// The watermark configuration.
	Watermark WatermarkConfiguration `yaml:"watermark"`
}

// NewObjectPoolOptions creates a new set of object pool options.
func (c *ObjectPoolConfiguration) NewObjectPoolOptions(
	instrumentOpts instrument.Options,
) ObjectPoolOptions {
	var (
		size    = _defaultSize
		dynamic bool
	)

	switch {
	case c.Size > 0:
		size = int(c.Size)
	case c.Size == _dynamicPoolSize:
		dynamic = true
	}

	return NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetSize(size).
		SetDynamic(dynamic).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
}

// BucketizedPoolConfiguration contains configuration for bucketized pools.
type BucketizedPoolConfiguration struct {
	// The pool bucket configuration.
	Buckets []BucketConfiguration `yaml:"buckets"`

	// The watermark configuration.
	Watermark WatermarkConfiguration `yaml:"watermark"`
}

// NewObjectPoolOptions creates a new set of object pool options.
func (c *BucketizedPoolConfiguration) NewObjectPoolOptions(
	instrumentOpts instrument.Options,
) ObjectPoolOptions {
	return NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
}

// NewBuckets create a new list of buckets.
func (c *BucketizedPoolConfiguration) NewBuckets() []Bucket {
	buckets := make([]Bucket, 0, len(c.Buckets))
	for _, bconfig := range c.Buckets {
		bucket := bconfig.NewBucket()
		buckets = append(buckets, bucket)
	}
	return buckets
}

// BucketConfiguration contains configuration for a pool bucket.
type BucketConfiguration struct {
	// The count of the items in the bucket.
	Count Size `yaml:"count"`

	// The capacity of each item in the bucket.
	Capacity int `yaml:"capacity"`
}

// NewBucket creates a new bucket.
func (c *BucketConfiguration) NewBucket() Bucket {
	return Bucket{
		Capacity: c.Capacity,
		Count:    c.Count,
	}
}

// WatermarkConfiguration contains watermark configuration for pools.
type WatermarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWatermark float64 `yaml:"low" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWatermark float64 `yaml:"high" validate:"min=0.0,max=1.0"`
}

// Size stores pool capacity for pools that can be either dynamic or pre-allocated
type Size int

// UnmarshalText unmarshals Size.
func (s *Size) UnmarshalText(b []byte) error {
	if string(b) == "dynamic" {
		*s = _dynamicPoolSize
		return nil
	}

	i, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}

	if i < 0 {
		return errors.New("pool capacity cannot be negative")
	}

	*s = Size(i)

	return nil
}
