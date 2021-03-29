// Copyright (c) 2020 Uber Technologies, Inc.
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

package ingest

import (
	"time"

	"github.com/uber-go/tally"
)

// LatencyBuckets are a set of latency buckets useful for measuring things.
type LatencyBuckets struct {
	WriteLatencyBuckets  tally.DurationBuckets
	IngestLatencyBuckets tally.DurationBuckets
}

// NewLatencyBuckets returns write and ingest latency buckets useful for
// measuring ingest latency (i.e. time from datapoint/sample created to time
// ingested) and write latency (i.e. time from received a sample from remote
// source to completion of that write locally).
func NewLatencyBuckets() (LatencyBuckets, error) {
	upTo1sBuckets, err := tally.LinearDurationBuckets(0, 100*time.Millisecond, 10)
	if err != nil {
		return LatencyBuckets{}, err
	}

	upTo10sBuckets, err := tally.LinearDurationBuckets(time.Second, 500*time.Millisecond, 18)
	if err != nil {
		return LatencyBuckets{}, err
	}

	upTo60sBuckets, err := tally.LinearDurationBuckets(10*time.Second, 5*time.Second, 11)
	if err != nil {
		return LatencyBuckets{}, err
	}

	upTo60mBuckets, err := tally.LinearDurationBuckets(0, 5*time.Minute, 12)
	if err != nil {
		return LatencyBuckets{}, err
	}
	upTo60mBuckets = upTo60mBuckets[1:] // Remove the first 0s to get 5 min aligned buckets

	upTo6hBuckets, err := tally.LinearDurationBuckets(time.Hour, 30*time.Minute, 12)
	if err != nil {
		return LatencyBuckets{}, err
	}

	upTo24hBuckets, err := tally.LinearDurationBuckets(6*time.Hour, time.Hour, 19)
	if err != nil {
		return LatencyBuckets{}, err
	}
	upTo24hBuckets = upTo24hBuckets[1:] // Remove the first 6h to get 1 hour aligned buckets

	var writeLatencyBuckets tally.DurationBuckets
	writeLatencyBuckets = append(writeLatencyBuckets, upTo1sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo10sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo60sBuckets...)
	writeLatencyBuckets = append(writeLatencyBuckets, upTo60mBuckets...)

	var ingestLatencyBuckets tally.DurationBuckets
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo1sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo10sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60sBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo60mBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo6hBuckets...)
	ingestLatencyBuckets = append(ingestLatencyBuckets, upTo24hBuckets...)

	return LatencyBuckets{
		WriteLatencyBuckets:  writeLatencyBuckets,
		IngestLatencyBuckets: ingestLatencyBuckets,
	}, nil
}
