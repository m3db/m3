package aggregator

import (
	"runtime"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"
)

func BenchmarkAggregationValues(b *testing.B) {
	aggregationKeys := []aggregationKey{
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz1234"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz55"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz45"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz1234"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz42"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
	}

	vals := make(aggregationValues, len(aggregationKeys))
	for i := range aggregationKeys {
		vals[i] = aggregationValue{key: aggregationKeys[i]}
	}

	b.ResetTimer()
	var contains bool
	for n := 0; n < b.N; n++ {
		contains = vals.contains(&aggregationKeys[len(aggregationKeys)-1])
	}
	runtime.KeepAlive(contains)
}
