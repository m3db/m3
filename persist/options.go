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

package persist

import "time"

const (
	// defaultThroughputLimitEnabled determines whether throughput limiting is enabled
	defaultThroughputLimitEnabled = false

	// defaultThroughputLimitMbps is the default throughput limit in Mb/s
	defaultThroughputLimitMbps = 50.0

	// defaultThroughputCheckInterval is the default throughput check interval
	defaultThroughputCheckInterval = 100 * time.Millisecond
)

type throughputLimitOptions struct {
	throughputLimitEnabled  bool
	throughputLimitMbps     float64
	throughputCheckInterval time.Duration
}

// NewThroughputLimitOptions creates a new throughput limit options
func NewThroughputLimitOptions() ThroughputLimitOptions {
	return &throughputLimitOptions{
		throughputLimitEnabled:  defaultThroughputLimitEnabled,
		throughputLimitMbps:     defaultThroughputLimitMbps,
		throughputCheckInterval: defaultThroughputCheckInterval,
	}
}

func (o *throughputLimitOptions) SetThroughputLimitEnabled(value bool) ThroughputLimitOptions {
	opts := *o
	opts.throughputLimitEnabled = value
	return &opts
}

func (o *throughputLimitOptions) ThroughputLimitEnabled() bool {
	return o.throughputLimitEnabled
}

func (o *throughputLimitOptions) SetThroughputLimitMbps(value float64) ThroughputLimitOptions {
	opts := *o
	opts.throughputLimitMbps = value
	return &opts
}

func (o *throughputLimitOptions) ThroughputLimitMbps() float64 {
	return o.throughputLimitMbps
}

func (o *throughputLimitOptions) SetThroughputCheckInterval(value time.Duration) ThroughputLimitOptions {
	opts := *o
	opts.throughputCheckInterval = value
	return &opts
}

func (o *throughputLimitOptions) ThroughputCheckInterval() time.Duration {
	return o.throughputCheckInterval
}
