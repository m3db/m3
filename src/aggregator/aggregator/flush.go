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

	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
)

// flushingMetricList periodically flushes metrics stored in the list for a given shard.
type flushingMetricList interface {
	// Shard returns the shard associated with the flusher.
	Shard() uint32

	// ID returns the list id.
	ID() metricListID

	// FlushInterval returns the periodic flush interval.
	FlushInterval() time.Duration

	// LastFlushedNanos returns the last flushed timestamp.
	LastFlushedNanos() int64

	// Flush performs a flush for a given request.
	Flush(req flushRequest)

	// DiscardBefore discards all metrics before a given timestamp.
	DiscardBefore(beforeNanos int64)
}

// fixedOffsetFlushingMetricList is a flushing metric list that flushes at fixed offset
// within the flush interval.
type fixedOffsetFlushingMetricList interface {
	flushingMetricList

	// FlushOffset is the fixed offset within the flush interval when
	// a flush is performed.
	FlushOffset() time.Duration
}

// flushRequest is a request to flush data.
type flushRequest struct {
	// The start time of consumable data.
	CutoverNanos int64

	// The end time of consumable data.
	CutoffNanos int64

	// If nonzero, data between [now - bufferAfterCutoff, now) are buffered.
	BufferAfterCutoff time.Duration
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
	metricType metric.Type,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
)

// A flushForwardedMetricFn flushes an aggregated metric datapoint eligible for
// forwarding by either forwarding it (potentially to a different aggregation
// server) or dropping it. Processing of the datapoint continues after it is
// flushed as required by the pipeline.
type flushForwardedMetricFn func(
	writeFn writeForwardedMetricFn,
	aggregationKey aggregationKey,
	timeNanos int64,
	value float64,
)

// An onForwardingElemFlushedFn is a callback function that should be called
// when an aggregation element producing forwarded metrics has been flushed.
type onForwardingElemFlushedFn func(
	onDoneFn onForwardedAggregationDoneFn,
	aggregationKey aggregationKey,
)
