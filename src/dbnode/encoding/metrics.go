// Copyright (c) 2021 Uber Technologies, Inc.
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

import "github.com/uber-go/tally"

// Metrics contains metrics for encoding.
type Metrics struct {
	TimestampEncoder TimestampEncoderMetrics
}

// NewMetrics returns new Metrics.
func NewMetrics(scope tally.Scope) Metrics {
	return Metrics{
		TimestampEncoder: newTimestampEncoderMetrics(scope.SubScope("timestamp-encoder")),
	}
}

// TimestampEncoderMetrics contains timestamp encoder metrics.
type TimestampEncoderMetrics struct {
	annotationRewritten tally.Counter
}

func newTimestampEncoderMetrics(scope tally.Scope) TimestampEncoderMetrics {
	return TimestampEncoderMetrics{
		annotationRewritten: scope.Counter("annotation-rewritten"),
	}
}

// IncAnnotationRewritten increments annotation rewritten counter.
func (m *TimestampEncoderMetrics) IncAnnotationRewritten() {
	if m.annotationRewritten != nil {
		m.annotationRewritten.Inc(1)
	}
}
