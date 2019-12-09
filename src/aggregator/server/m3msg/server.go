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

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/sampler"

	"go.uber.org/zap"
)

// NewPassThroughWriteFn returns the m3msg write function for pass-through metrics.
func NewPassThroughWriteFn(
	aggregator aggregator.Aggregator,
	s *sampler.Sampler,
	log *zap.Logger,
) m3msg.WriteFn {
	return func(
		ctx context.Context,
		id []byte,
		metricNanos, encodeNanos int64,
		value float64,
		sp policy.StoragePolicy,
		callback m3msg.Callbackable,
	) {
		// The type of a pass-through metric does not matter as it is written directly into m3db.
		metric := aggregated.Metric{
			Type:      metric.GaugeType,
			ID:        id,
			TimeNanos: metricNanos,
			Value:     value,
		}
		metadata := metadata.TimedMetadata{
			AggregationID: aggregation.MustCompressTypes(aggregation.Last),
			StoragePolicy: sp,
		}

		if err := aggregator.AddPassThrough(metric, metadata); err != nil {
			log.Info("[FAIL] to write pass-through metric",
				zap.String("metric", metric.String()),
				zap.String("aggregationID", metadata.AggregationID.String()),
				zap.String("storagePolicy", metadata.StoragePolicy.String()),
			)
			callback.Callback(m3msg.OnRetriableError)
		}

		if s != nil && s.Sample() {
			log.Info("[SUCCESS] to write pass-through metric (sampled)",
				zap.String("metric", metric.String()),
				zap.String("aggregationID", metadata.AggregationID.String()),
				zap.String("storagePolicy", metadata.StoragePolicy.String()),
			)
		}
		callback.Callback(m3msg.OnSuccess)
	}
}

type addPassThroughError struct {
	err error
}

func toAddPassThroughError(err error) error {
	if err == nil {
		return nil
	}
	return addPassThroughError{
		err: err,
	}
}

func (e addPassThroughError) Error() string { return e.err.Error() }
