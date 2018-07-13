// Copyright (c) 2018 Uber Technologies, Inc.
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

package aggregator

import (
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
)

const (
	forwardingLatencyBucketVersion = 4
	numForwardingLatencyBuckets    = 40
)

// ForwardingLatencyBucketsFn creates forwarding latency buckets.
type ForwardingLatencyBucketsFn func(
	key ForwardingLatencyBucketKey,
	numLatencyBuckets int,
) tally.Buckets

// ForwardingLatencyBucketKey is the key for a forwarding latency bucket.
type ForwardingLatencyBucketKey struct {
	Resolution        time.Duration
	NumForwardedTimes int
}

// ForwardingLatencyHistograms record forwarded latencies as histograms.
type ForwardingLatencyHistograms struct {
	sync.RWMutex

	scope      tally.Scope
	bucketsFn  ForwardingLatencyBucketsFn
	histograms map[ForwardingLatencyBucketKey]tally.Histogram
}

// NewForwardingLatencyHistograms create a new set of forwarding latency histograms.
func NewForwardingLatencyHistograms(
	scope tally.Scope,
	bucketsFn ForwardingLatencyBucketsFn,
) *ForwardingLatencyHistograms {
	return &ForwardingLatencyHistograms{
		scope:      scope,
		bucketsFn:  bucketsFn,
		histograms: make(map[ForwardingLatencyBucketKey]tally.Histogram),
	}
}

// RecordDuration record a forwarding latency.
func (m *ForwardingLatencyHistograms) RecordDuration(
	resolution time.Duration,
	numForwardedTimes int,
	duration time.Duration,
) {
	key := ForwardingLatencyBucketKey{
		Resolution:        resolution,
		NumForwardedTimes: numForwardedTimes,
	}
	m.RLock()
	histogram, exists := m.histograms[key]
	m.RUnlock()
	if exists {
		histogram.RecordDuration(duration)
		return
	}
	m.Lock()
	histogram, exists = m.histograms[key]
	if exists {
		m.Unlock()
		histogram.RecordDuration(duration)
		return
	}
	buckets := m.bucketsFn(key, numForwardingLatencyBuckets)
	histogram = m.scope.Tagged(map[string]string{
		"bucket-version":      strconv.Itoa(forwardingLatencyBucketVersion),
		"resolution":          resolution.String(),
		"num-forwarded-times": strconv.Itoa(numForwardedTimes),
	}).Histogram("forwarding-latency", buckets)
	m.histograms[key] = histogram
	m.Unlock()
	histogram.RecordDuration(duration)
}
