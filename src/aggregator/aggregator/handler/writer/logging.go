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

package writer

import (
	"github.com/m3db/m3/src/metrics/metric/aggregated"

	"go.uber.org/zap"
)

type loggingWriter struct {
	logger *zap.Logger
}

// NewLoggingWriter creates a new logging writer.
func NewLoggingWriter(logger *zap.Logger) Writer {
	return loggingWriter{logger: logger}
}

func (w loggingWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	w.logger.Info("aggregated metric with policy",
		zap.Stringer("metric", mp.ChunkedMetric),
		zap.Stringer("policy", mp.StoragePolicy),
	)
	return nil
}

func (w loggingWriter) Flush() error { return nil }
func (w loggingWriter) Close() error { return nil }
