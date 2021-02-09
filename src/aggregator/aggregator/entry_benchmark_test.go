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
		contains = vals.contains(aggregationKeys[len(aggregationKeys)-1])
	}
	runtime.KeepAlive(contains)
}
