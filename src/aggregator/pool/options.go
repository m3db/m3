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

const (
	defaultSize = 4096
)

var (
	defaultBuckets = []Bucket{
		{Capacity: 16, Count: 4096},
	}
)

type objectPoolOptions struct {
	size  int
	scope tally.Scope
}

// NewObjectPoolOptions creates a new set of object pool options
func NewObjectPoolOptions() ObjectPoolOptions {
	return objectPoolOptions{
		size:  defaultSize,
		scope: tally.NoopScope,
	}
}

func (o objectPoolOptions) SetSize(value int) ObjectPoolOptions {
	o.size = value
	return o
}

func (o objectPoolOptions) Size() int {
	return o.size
}

func (o objectPoolOptions) SetMetricsScope(value tally.Scope) ObjectPoolOptions {
	o.scope = value
	return o
}

func (o objectPoolOptions) MetricsScope() tally.Scope {
	return o.scope
}

type bucketizedObjectPoolOptions struct {
	buckets []Bucket
	scope   tally.Scope
}

// NewBucketizedObjectPoolOptions creates a new set of bucket pool options
func NewBucketizedObjectPoolOptions() BucketizedObjectPoolOptions {
	return bucketizedObjectPoolOptions{
		buckets: defaultBuckets,
		scope:   tally.NoopScope,
	}
}

func (o bucketizedObjectPoolOptions) SetBuckets(buckets []Bucket) BucketizedObjectPoolOptions {
	o.buckets = buckets
	return o
}

func (o bucketizedObjectPoolOptions) Buckets() []Bucket {
	return o.buckets
}

func (o bucketizedObjectPoolOptions) SetMetricsScope(value tally.Scope) BucketizedObjectPoolOptions {
	o.scope = value
	return o
}

func (o bucketizedObjectPoolOptions) MetricsScope() tally.Scope {
	return o.scope
}
