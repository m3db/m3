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
	"fmt"
	"testing"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/transformation"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

var (
	testSmallPipeline = NewPipeline([]OpUnion{
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
				ID:            []byte("foo"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	testLargePipeline = NewPipeline([]OpUnion{
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
				ID:            []byte("bar"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
			},
		},
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.Absolute,
			},
		},
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
				ID:            []byte("baz"),
				AggregationID: aggregation.MustCompressTypes(aggregation.P99),
			},
		},
	})
	testBadPipeline = NewPipeline([]OpUnion{
		{
			Type: pipeline.UnknownOpType,
		},
	})
	testSmallPipelineProto = pipelinepb.AppliedPipeline{
		Ops: []pipelinepb.AppliedPipelineOp{
			{
				Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
				Transformation: &pipelinepb.TransformationOp{
					Type: transformationpb.TransformationType_PERSECOND,
				},
			},
			{
				Type: pipelinepb.AppliedPipelineOp_ROLLUP,
				Rollup: &pipelinepb.AppliedRollupOp{
					Id:            []byte("foo"),
					AggregationId: aggregationpb.AggregationID{Id: 0},
				},
			},
		},
	}
	testLargePipelineProto = pipelinepb.AppliedPipeline{
		Ops: []pipelinepb.AppliedPipelineOp{
			{
				Type: pipelinepb.AppliedPipelineOp_ROLLUP,
				Rollup: &pipelinepb.AppliedRollupOp{
					Id:            []byte("bar"),
					AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum)[0]},
				},
			},
			{
				Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
				Transformation: &pipelinepb.TransformationOp{
					Type: transformationpb.TransformationType_ABSOLUTE,
				},
			},
			{
				Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
				Transformation: &pipelinepb.TransformationOp{
					Type: transformationpb.TransformationType_PERSECOND,
				},
			},
			{
				Type: pipelinepb.AppliedPipelineOp_ROLLUP,
				Rollup: &pipelinepb.AppliedRollupOp{
					Id:            []byte("baz"),
					AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.P99)[0]},
				},
			},
		},
	}
	testBadPipelineProto = pipelinepb.AppliedPipeline{
		Ops: []pipelinepb.AppliedPipelineOp{
			{
				Type: pipelinepb.AppliedPipelineOp_UNKNOWN,
			},
		},
	}
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
			p:        NewPipeline([]OpUnion{}),
			expected: true,
		},
		{
			p: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
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
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.Absolute,
					},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
					},
				},
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.Absolute,
					},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
					},
				},
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: true,
		},
		{
			p1: NewPipeline(nil),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.Absolute,
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.PerSecond,
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{
						Type: transformation.Absolute,
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("bar"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			expected: false,
		},
		{
			p1: NewPipeline([]OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.P99),
					},
				},
			}),
			p2: NewPipeline([]OpUnion{
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			expected: false,
		},
	}

	for _, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("%v %v", input.p1.String(), input.p2.String()), func(t *testing.T) {
			require.Equal(t, input.expected, input.p1.Equal(input.p2))
			require.Equal(t, input.expected, input.p2.Equal(input.p1))
			// assert implementation is equal to OpUnion
			if input.expected {
				for i, op := range input.p1.Operations {
					require.True(t, op.Equal(input.p2.Operations[i]))
				}
				for i, op := range input.p2.Operations {
					require.True(t, op.Equal(input.p1.Operations[i]))
				}
			} else if len(input.p1.Operations) == len(input.p2.Operations) {
				for i, op := range input.p1.Operations {
					require.False(t, op.Equal(input.p2.Operations[i]))
				}
				for i, op := range input.p2.Operations {
					require.False(t, op.Equal(input.p1.Operations[i]))
				}
			}
		})
	}
}

func TestPipelineCloneEmptyPipeline(t *testing.T) {
	p1 := Pipeline{}
	require.True(t, p1.IsEmpty())

	p2 := p1.Clone()
	require.True(t, p1.Equal(p2))

	p2.Operations = append(p2.Operations, OpUnion{
		Type: pipeline.RollupOpType,
		Rollup: RollupOp{
			ID:            []byte("foo"),
			AggregationID: aggregation.MustCompressTypes(aggregation.P99),
		},
	})
	require.False(t, p1.Equal(p2))
	require.True(t, p1.IsEmpty())
}

func TestPipelineCloneMultiLevelPipeline(t *testing.T) {
	p1 := NewPipeline([]OpUnion{
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.Absolute,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
				ID:            []byte("foo"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
			},
		},
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
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
	p2.Operations[0].Transformation.Type = transformation.PerSecond
	p2.Operations[1].Rollup.ID[0] = 'z'
	p2.Operations[3].Rollup.AggregationID = aggregation.MustCompressTypes(aggregation.Count)

	// Verify the mutations do not affect the source pipeline or other clones.
	require.False(t, p1.Equal(p2))
	require.False(t, p2.Equal(p3))
	require.True(t, p1.Equal(p3))
	require.Equal(t, transformation.Absolute, p1.At(0).Transformation.Type)
	require.Equal(t, []byte("foo"), p1.At(1).Rollup.ID)
	require.Equal(t, aggregation.MustCompressTypes(aggregation.P99), p1.At(3).Rollup.AggregationID)
}

func TestPipelineSubPipeline(t *testing.T) {
	operations := []OpUnion{
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.Absolute,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
				ID:            []byte("foo"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
			},
		},
		{
			Type: pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{
				Type: transformation.PerSecond,
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: RollupOp{
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
			expected:       NewPipeline([]OpUnion{}),
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
			p: NewPipeline([]OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: RollupOp{
						ID:            []byte("foo"),
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					},
				},
			}),
			expected: "{operations: [{transformation: PerSecond}, {rollup: {id: foo, aggregation: Sum}}]}",
		},
		{
			p: NewPipeline([]OpUnion{
				{
					Type: pipeline.OpType(10),
				},
			}),
			expected: "{operations: [{unknown op type: OpType(10)}]}",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestPipelineToProto(t *testing.T) {
	inputs := []struct {
		sequence []Pipeline
		expected []pipelinepb.AppliedPipeline
	}{
		{
			sequence: []Pipeline{
				testSmallPipeline,
				testLargePipeline,
			},
			expected: []pipelinepb.AppliedPipeline{
				testSmallPipelineProto,
				testLargePipelineProto,
			},
		},
		{
			sequence: []Pipeline{
				testLargePipeline,
				testSmallPipeline,
			},
			expected: []pipelinepb.AppliedPipeline{
				testLargePipelineProto,
				testSmallPipelineProto,
			},
		},
	}

	for _, input := range inputs {
		var pb pipelinepb.AppliedPipeline
		for i, pipeline := range input.sequence {
			require.NoError(t, pipeline.ToProto(&pb))
			require.Equal(t, input.expected[i], pb)
		}
	}
}

func TestPipelineToProtoBadPipeline(t *testing.T) {
	var pb pipelinepb.AppliedPipeline
	require.Error(t, testBadPipeline.ToProto(&pb))
}

func TestPipelineFromProto(t *testing.T) {
	inputs := []struct {
		sequence []pipelinepb.AppliedPipeline
		expected []Pipeline
	}{
		{
			sequence: []pipelinepb.AppliedPipeline{
				testSmallPipelineProto,
				testLargePipelineProto,
			},
			expected: []Pipeline{
				testSmallPipeline,
				testLargePipeline,
			},
		},
		{
			sequence: []pipelinepb.AppliedPipeline{
				testLargePipelineProto,
				testSmallPipelineProto,
			},
			expected: []Pipeline{
				testLargePipeline,
				testSmallPipeline,
			},
		},
	}

	for _, input := range inputs {
		var res Pipeline
		for i, pb := range input.sequence {
			require.NoError(t, res.FromProto(pb))
			if !cmp.Equal(input.expected[i], res) {
				t.Log(cmp.Diff(input.expected[i], res))
				t.Fail()
			}
		}
	}
}

func TestPipelineFromProtoBadPipelineProto(t *testing.T) {
	var res Pipeline
	require.Error(t, res.FromProto(testBadPipelineProto))
}

func TestPipelineRoundTrip(t *testing.T) {
	inputs := [][]Pipeline{
		{
			testSmallPipeline,
			testLargePipeline,
		},
		{
			testLargePipeline,
			testSmallPipeline,
		},
	}

	for _, input := range inputs {
		var (
			pb  pipelinepb.AppliedPipeline
			res Pipeline
		)
		for _, pipeline := range input {
			require.NoError(t, pipeline.ToProto(&pb))
			require.NoError(t, res.FromProto(pb))
			if !cmp.Equal(pipeline, res) {
				t.Log(cmp.Diff(pipeline, res))
				t.Fail()
			}
		}
	}
}

func TestPipeline_WithResets(t *testing.T) {
	p := Pipeline{
		Operations: []OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Add,
				},
			},
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.PerSecond,
				},
			},
		},
	}
	copyP := p
	expected := Pipeline{
		Operations: []OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Reset,
				},
			},
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.PerSecond,
				},
			},
		},
	}
	require.Equal(t, expected, p.WithResets())
	// make sure the original pipeline doesn't change.
	require.Equal(t, copyP, p)
}
