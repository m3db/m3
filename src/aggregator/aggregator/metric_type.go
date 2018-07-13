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

// IncomingMetricType describes the type of an incoming metric.
type IncomingMetricType int

const (
	// UnknownIncomingMetric is an incoming metric with unknown type.
	UnknownIncomingMetric IncomingMetricType = iota

	// StandardIncomingMetric is a standard (currently untimed) incoming metric.
	StandardIncomingMetric

	// ForwardedIncomingMetric is a forwarded incoming metric.
	ForwardedIncomingMetric
)

func (t IncomingMetricType) String() string {
	switch t {
	case StandardIncomingMetric:
		return "standardIncomingMetric"
	case ForwardedIncomingMetric:
		return "forwardedIncomingMetric"
	default:
		// nolint: goconst
		// Should never get here.
		return "unknown"
	}
}

type outgoingMetricType int

const (
	// localOutgoingMetric is an outgoing metric that gets flushed to backends
	// locally known to the server.
	localOutgoingMetric outgoingMetricType = iota

	// forwardedOutgoingMetric is an outgoing metric that gets forwarded to
	// other aggregation servers for further aggregation and rollup.
	forwardedOutgoingMetric
)

func (t outgoingMetricType) String() string {
	switch t {
	case localOutgoingMetric:
		return "localOutgoingMetric"
	case forwardedOutgoingMetric:
		return "forwardedOutgoingMetric"
	default:
		// nolint: goconst
		// Should never get here.
		return "unknown"
	}
}
