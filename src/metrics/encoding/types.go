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

package encoding

import (
	"io"

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
)

// UnaggregatedMessageType is the unaggregated message type.
type UnaggregatedMessageType int

// A list of supported unaggregated message type.
const (
	UnknownMessageType UnaggregatedMessageType = iota
	CounterWithMetadatasType
	BatchTimerWithMetadatasType
	GaugeWithMetadatasType
	TimedMetricWithForwardMetadataType
)

// UnaggregatedMessageUnion is a union of different types of unaggregated messages.
// A message union may contain at most one type of message that is determined
// by the `Type` field of the union, which in turn determines which one
// of the field in the union contains the corresponding message data.
type UnaggregatedMessageUnion struct {
	Type                           UnaggregatedMessageType
	CounterWithMetadatas           unaggregated.CounterWithMetadatas
	BatchTimerWithMetadatas        unaggregated.BatchTimerWithMetadatas
	GaugeWithMetadatas             unaggregated.GaugeWithMetadatas
	TimedMetricWithForwardMetadata aggregated.MetricWithForwardMetadata
}

// ByteReadScanner is capable of reading and scanning bytes.
type ByteReadScanner interface {
	io.Reader
	io.ByteScanner
}
