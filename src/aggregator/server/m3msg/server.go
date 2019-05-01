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
)

// GetWriteFn returns the m3msg write function for pass-through metrics
func GetWriteFn(
	aggregator aggregator.Aggregator,
	s *sampler.Sampler,
) m3msg.WriteFn {
	return func(
		ctx context.Context,
		id []byte,
		metricNanos, encodeNanos int64,
		value float64,
		sp policy.StoragePolicy,
		callback m3msg.Callbackable,
	) {
		// todo: check 10s:2d storage policy
		// todo: s.Sample()

		// Get metric and metadata
		// fishie9: the type of a pass-through metric does not matter as they will be written directly into m3db
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
			callback.Callback(m3msg.OnRetriableError)
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
