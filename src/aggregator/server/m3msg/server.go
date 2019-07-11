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

// GetWriteFn returns the m3msg write function for pass-through metrics
func GetWriteFn(
	aggregator aggregator.Aggregator,
	s *sampler.Sampler,
	log *zap.SugaredLogger,
) m3msg.WriteFn {
	return func(
		ctx context.Context,
		id []byte,
		metricNanos, encodeNanos int64,
		value float64,
		sp policy.StoragePolicy,
		callback m3msg.Callbackable,
	) {
		// The type of a pass-through metric does not matter as it is written directly into m3db
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
			if log != nil {
				log.Infof("[FAIL] to write pass-through metric %s with metadata ID=%s SP=%s",
					metric.String(),
					metadata.AggregationID.String(),
					metadata.StoragePolicy.String(),
				)
			}
			callback.Callback(m3msg.OnRetriableError)
		}

		if s != nil && s.Sample() && log != nil {
			log.Infof("[SUCCESS] to write pass-through metric %s with metadata ID=%s SP=%s (sampled)",
				metric.String(),
				metadata.AggregationID.String(),
				metadata.StoragePolicy.String(),
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
