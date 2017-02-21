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

package xcounter

import "time"

const (
	defaultNumBuckets = 60
	defaultInterval   = time.Second
)

// Options controls the parameters for frequency counters
type Options struct {
	numBuckets int
	interval   time.Duration
}

// NewOptions creates new options
func NewOptions() Options {
	return Options{
		numBuckets: defaultNumBuckets,
		interval:   defaultInterval,
	}
}

// NumBuckets returns the number of buckets
func (o Options) NumBuckets() int { return o.numBuckets }

// Interval returns the interval associated with each bucket
func (o Options) Interval() time.Duration { return o.interval }

// SetNumBuckets sets the number of buckets
func (o Options) SetNumBuckets(value int) Options {
	o.numBuckets = value
	return o
}

// SetInterval sets the interval
func (o Options) SetInterval(value time.Duration) Options {
	o.interval = value
	return o
}
