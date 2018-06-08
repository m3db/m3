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

package aggregator

import (
	"time"

	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
)

// FlushRequest is a request to flush data.
type FlushRequest struct {
	// The start time of consumable data.
	CutoverNanos int64

	// The end time of consumable data.
	CutoffNanos int64

	// If nonzero, data between [now - bufferAfterCutoff, now) are buffered.
	BufferAfterCutoff time.Duration
}

// PeriodicFlusher flushes metrics periodically.
type PeriodicFlusher interface {
	// Shard returns the shard associated with the flusher.
	Shard() uint32

	// Resolution returns the resolution of metrics associated with the flusher.
	Resolution() time.Duration

	// FlushInterval returns the periodic flush interval.
	FlushInterval() time.Duration

	// LastFlushedNanos returns the last flushed timestamp.
	LastFlushedNanos() int64

	// Flush performs a flush for a given request.
	Flush(req FlushRequest)

	// DiscardBefore discards all metrics before a given timestamp.
	DiscardBefore(beforeNanos int64)
}

type flushType int

const (
	consumeType flushType = iota
	discardType
)

// A flushLocalMetricFn flushes an aggregated metric datapoint locally by either
// consuming or discarding it. Processing of the datapoint is completed once it is
// flushed.
type flushLocalMetricFn func(
	idPrefix []byte,
	id id.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
)

// A flushForwardMetricFn flushes an aggregated metric datapoint eligible for
// forwarding by either forwarding it (potentially to a different aggregation
// server) or dropping it. Processing of the datapoint continues after it is
// flushed as required by the pipeline.
type flushForwardMetricFn func(
	metric aggregated.Metric,
	meta metadata.ForwardMetadata,
)
