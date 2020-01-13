// Copyright (c) 2019 Uber Technologies, Inc.
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

package m3msg

import (
	"context"
	"errors"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/server"

	"go.uber.org/zap"
)

var (
	errServerAddressEmpty = errors.New("m3msg server address is empty")
)

// newPassThroughWriteFn returns the m3msg write function for pass-through metrics.
func newPassThroughWriteFn(
	aggregator aggregator.Aggregator,
) m3msg.WriteFn {
	return func(
		ctx context.Context,
		id []byte,
		metricNanos, encodeNanos int64,
		value float64,
		sp policy.StoragePolicy,
		callback m3msg.Callbackable,
	) {
		// The type of a pass-through metric does not matter.
		metric := aggregated.Metric{
			Type:      metric.GaugeType,
			ID:        id,
			TimeNanos: metricNanos,
			Value:     value,
		}
		metadata := metadata.TimedMetadata{
			StoragePolicy: sp,
		}

		if err := aggregator.AddPassThrough(metric, metadata); err != nil {
			// nb: if we want to apply back pressure upstream (in this case the collector
			// producting the metrics), we need to propagate the retryability of this error.
			// Currently, we drop metrics upon the floor in case of errors.
			callback.Callback(m3msg.OnNonRetriableError)
			return
		}

		callback.Callback(m3msg.OnSuccess)
	}
}

// NewPassThroughServer returns a m3msg server for pass-through metrics.
func NewPassThroughServer(
	cfg *m3msg.Configuration,
	agg aggregator.Aggregator,
	iOpts instrument.Options,
) (server.Server, error) {
	if cfg == nil || cfg.Server.ListenAddress == "" {
		return nil, errServerAddressEmpty
	}
	iOpts = iOpts.SetLogger(iOpts.Logger().With(zap.String("from", "passthru-server")))
	return cfg.NewServer(newPassThroughWriteFn(agg), iOpts)
}
