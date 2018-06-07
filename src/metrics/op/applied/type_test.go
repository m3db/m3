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

package applied

import (
	"testing"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/transformation"

	"github.com/stretchr/testify/require"
)

func TestPipelineIsEmpty(t *testing.T) {
	inputs := []struct {
		p        Pipeline
		expected bool
	}{
		{
			p:        NewPipeline(nil),
			expected: true,
		},
		{
			p:        NewPipeline([]Union{}),
			expected: true,
		},
		{
			p: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.Absolute,
					},
				}}),
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.IsEmpty())
	}
}

func TestPipelineEqual(t *testing.T) {
	inputs := []struct {
		p1       Pipeline
		p2       Pipeline
		expected bool
	}{
		{
			p1:       NewPipeline(nil),
			p2:       NewPipeline(nil),
			expected: true,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.Absolute,
					},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
					},
				},
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.Absolute,
					},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
					},
				},
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: true,
		},
		{
			p1: NewPipeline(nil),
			p2: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.Absolute,
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.TransformationType,
					Transformation: op.Transformation{
						Type: transformation.Absolute,
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]Union{
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]Union{
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p1.Equal(input.p2))
		require.Equal(t, input.expected, input.p2.Equal(input.p1))
	}
}

func TestPipelineCloneEmptyPipeline(t *testing.T) {
	p1 := Pipeline{}
	require.True(t, p1.IsEmpty())

	p2 := p1.Clone()
	require.True(t, p1.Equal(p2))

	p2.operations = append(p2.operations, Union{
		Type: op.RollupType,
		Rollup: Rollup{
			ID:            []byte("foo"),
			AggregationID: aggregation.MustCompressTypes(aggregation.P99),
		},
	})
	require.False(t, p1.Equal(p2))
	require.True(t, p1.IsEmpty())
}

func TestPipelineCloneMultiLevelPipeline(t *testing.T) {
	p1 := NewPipeline([]Union{
		{
			Type: op.TransformationType,
			Transformation: op.Transformation{
				Type: transformation.Absolute,
			},
		},
		{
			Type: op.RollupType,
			Rollup: Rollup{
				ID:            []byte("foo"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
			},
		},
		{
			Type: op.TransformationType,
			Transformation: op.Transformation{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: op.RollupType,
			Rollup: Rollup{
				ID:            []byte("bar"),
				AggregationID: aggregation.MustCompressTypes(aggregation.P99),
			},
		},
	})
	p2 := p1.Clone()
	p3 := p2.Clone()
	require.True(t, p1.Equal(p2))
	require.True(t, p1.Equal(p3))

	// Mutate the operations of a cloned pipeline.
	p2.operations[0].Transformation.Type = transformation.PerSecond
	p2.operations[1].Rollup.ID[0] = 'z'
	p2.operations[3].Rollup.AggregationID = aggregation.MustCompressTypes(aggregation.Count)

	// Verify the mutations do not affect the source pipeline or other clones.
	require.False(t, p1.Equal(p2))
	require.False(t, p2.Equal(p3))
	require.True(t, p1.Equal(p3))
	require.Equal(t, transformation.Absolute, p1.At(0).Transformation.Type)
	require.Equal(t, []byte("foo"), p1.At(1).Rollup.ID)
	require.Equal(t, aggregation.MustCompressTypes(aggregation.P99), p1.At(3).Rollup.AggregationID)
}

func TestPipelineSubPipeline(t *testing.T) {
	operations := []Union{
		{
			Type: op.TransformationType,
			Transformation: op.Transformation{
				Type: transformation.Absolute,
			},
		},
		{
			Type: op.RollupType,
			Rollup: Rollup{
				ID:            []byte("foo"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
			},
		},
		{
			Type: op.TransformationType,
			Transformation: op.Transformation{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: op.RollupType,
			Rollup: Rollup{
				ID:            []byte("bar"),
				AggregationID: aggregation.MustCompressTypes(aggregation.P99),
			},
		},
	}
	p := NewPipeline(operations)
	inputs := []struct {
		startInclusive int
		endExclusive   int
		expected       Pipeline
	}{
		{
			startInclusive: 0,
			endExclusive:   0,
			expected:       NewPipeline([]Union{}),
		},
		{
			startInclusive: 0,
			endExclusive:   4,
			expected:       NewPipeline(operations),
		},
		{
			startInclusive: 1,
			endExclusive:   3,
			expected:       NewPipeline(operations[1:3]),
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, p.SubPipeline(input.startInclusive, input.endExclusive))
	}
}

func TestPipelineString(t *testing.T) {
	inputs := []struct {
		p        Pipeline
		expected string
	}{
		{
			p: NewPipeline([]Union{
				{
					Type:           op.TransformationType,
					Transformation: op.Transformation{Type: transformation.PerSecond},
				},
				{
					Type: op.RollupType,
					Rollup: Rollup{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			expected: "{operations: [{transformation: PerSecond}, {rollup: {id: foo, aggregation: Sum}}]}",
		},
		{
			p: NewPipeline([]Union{
				{
					Type: op.Type(10),
				},
			}),
			expected: "{operations: [{unknown op type: Type(10)}]}",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}
