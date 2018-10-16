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

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestAggregationKeyEqual(t *testing.T) {
	inputs := []struct {
		a        aggregationKey
		b        aggregationKey
		expected bool
	}{
		{
			a:        aggregationKey{},
			b:        aggregationKey{},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID:      aggregation.DefaultID,
				storagePolicy:      policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				idPrefixSuffixType: WithPrefixWithSuffix,
			},
			b: aggregationKey{
				aggregationID:      aggregation.DefaultID,
				storagePolicy:      policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				idPrefixSuffixType: WithPrefixWithSuffix,
			},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID:      aggregation.DefaultID,
				storagePolicy:      policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				idPrefixSuffixType: NoPrefixNoSuffix,
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			expected: false,
		},
		{
			a: aggregationKey{
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
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			b: aggregationKey{
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
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			b: aggregationKey{
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			expected: false,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 48*time.Hour),
			},
			expected: false,
		},
		{
			a: aggregationKey{
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
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			b: aggregationKey{
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
							ID:            []byte("foo.bar"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a.Equal(input.b))
		require.Equal(t, input.expected, input.b.Equal(input.a))
	}
}
