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

package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/metrics/x/bytes"
	"github.com/m3db/m3/src/x/test/testmarshal"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

var (
	testTransformationOp = TransformationOp{
		Type: transformation.PerSecond,
	}
	testTransformationOpProto = pipelinepb.TransformationOp{
		Type: transformationpb.TransformationType_PERSECOND,
	}
	testBadTransformationOpProto = pipelinepb.TransformationOp{
		Type: transformationpb.TransformationType_UNKNOWN,
	}
)

func TestAggregationOpEqual(t *testing.T) {
	inputs := []struct {
		a1       AggregationOp
		a2       AggregationOp
		expected bool
	}{
		{
			a1:       AggregationOp{aggregation.Count},
			a2:       AggregationOp{aggregation.Count},
			expected: true,
		},
		{
			a1:       AggregationOp{aggregation.Count},
			a2:       AggregationOp{aggregation.Sum},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestAggregationOpMarshalling(t *testing.T) {
	examples := []AggregationOp{{aggregation.Count}}

	t.Run("roundtrips", func(t *testing.T) {
		testmarshal.TestMarshalersRoundtrip(t, examples, []testmarshal.Marshaler{testmarshal.JSONMarshaler, testmarshal.YAMLMarshaler, testmarshal.TextMarshaler})
	})

	t.Run("marshals", func(t *testing.T) {
		cases := []struct {
			Example AggregationOp
			YAML    string
			JSON    string
			Text    string
		}{{
			Example: AggregationOp{aggregation.Count},

			Text: "Count",
			JSON: `"Count"`,
			YAML: "Count\n",
		}}

		t.Run("text", func(t *testing.T) {
			for _, tc := range cases {
				testmarshal.Require(t, testmarshal.AssertUnmarshals(t, testmarshal.TextMarshaler, tc.Example, []byte(tc.Text)))
				testmarshal.Require(t, testmarshal.AssertMarshals(t, testmarshal.TextMarshaler, tc.Example, []byte(tc.Text)))
			}
		})

		t.Run("json", func(t *testing.T) {
			for _, tc := range cases {
				testmarshal.Require(t, testmarshal.AssertUnmarshals(t, testmarshal.JSONMarshaler, tc.Example, []byte(tc.JSON)))
				testmarshal.Require(t, testmarshal.AssertMarshals(t, testmarshal.JSONMarshaler, tc.Example, []byte(tc.JSON)))
			}
		})

		t.Run("yaml", func(t *testing.T) {
			for _, tc := range cases {
				testmarshal.Require(t, testmarshal.AssertMarshals(t, testmarshal.YAMLMarshaler, tc.Example, []byte(tc.YAML)))
			}
		})
	})
}

func TestTransformationOpEqual(t *testing.T) {
	inputs := []struct {
		a1       TransformationOp
		a2       TransformationOp
		expected bool
	}{
		{
			a1:       TransformationOp{transformation.Absolute},
			a2:       TransformationOp{transformation.Absolute},
			expected: true,
		},
		{
			a1:       TransformationOp{transformation.Absolute},
			a2:       TransformationOp{transformation.PerSecond},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestTransformationOpClone(t *testing.T) {
	source := TransformationOp{transformation.Absolute}
	clone := source.Clone()
	require.Equal(t, source, clone)
	clone.Type = transformation.PerSecond
	require.Equal(t, transformation.Absolute, source.Type)
}

func TestPipelineString(t *testing.T) {
	inputs := []struct {
		p        Pipeline
		expected string
	}{
		{
			p: Pipeline{
				operations: []OpUnion{
					{
						Type:        AggregationOpType,
						Aggregation: AggregationOp{Type: aggregation.Last},
					},
					{
						Type:           TransformationOpType,
						Transformation: TransformationOp{Type: transformation.PerSecond},
					},
					{
						Type: RollupOpType,
						Rollup: RollupOp{
							NewName:       b("foo"),
							Tags:          [][]byte{b("tag1"), b("tag2")},
							AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						},
					},
				},
			},
			expected: "{operations: [{aggregation: Last}, {transformation: PerSecond}, {rollup: {name: foo, tags: [tag1, tag2], aggregation: Sum}}]}",
		},
		{
			p: Pipeline{
				operations: []OpUnion{
					{
						Type: OpType(10),
					},
				},
			},
			expected: "{operations: [{unknown op type: OpType(10)}]}",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestTransformationOpToProto(t *testing.T) {
	var pb pipelinepb.TransformationOp
	require.NoError(t, testTransformationOp.ToProto(&pb))
	require.Equal(t, testTransformationOpProto, pb)
}

func TestTransformationOpFromProto(t *testing.T) {
	var res TransformationOp
	require.NoError(t, res.FromProto(&testTransformationOpProto))
	require.Equal(t, testTransformationOp, res)
}

func TestTransformationOpFromProtoNilProto(t *testing.T) {
	var res TransformationOp
	require.Equal(t, errNilTransformationOpProto, res.FromProto(nil))
}

func TestTransformationOpFromProtoBadProto(t *testing.T) {
	var res TransformationOp
	require.Error(t, res.FromProto(&testBadTransformationOpProto))
}

func TestTransformationOpRoundTrip(t *testing.T) {
	var (
		pb  pipelinepb.TransformationOp
		res TransformationOp
	)
	require.NoError(t, testTransformationOp.ToProto(&pb))
	require.NoError(t, res.FromProto(&pb))
	require.Equal(t, testTransformationOp, res)
}

func TestRollupOpSameTransform(t *testing.T) {
	rollupOp := RollupOp{
		NewName: b("foo"),
		Tags:    bs("bar1", "bar2"),
	}
	inputs := []struct {
		op     RollupOp
		result bool
	}{
		{
			op:     RollupOp{NewName: b("foo"), Tags: bs("bar1", "bar2")},
			result: true,
		},
		{
			op:     RollupOp{NewName: b("foo"), Tags: bs("bar2", "bar1")},
			result: true,
		},
		{
			op:     RollupOp{NewName: b("foo"), Tags: bs("bar1")},
			result: false,
		},
		{
			op:     RollupOp{NewName: b("foo"), Tags: bs("bar1", "bar2", "bar3")},
			result: false,
		},
		{
			op:     RollupOp{NewName: b("foo"), Tags: bs("bar1", "bar3")},
			result: false,
		},
		{
			op:     RollupOp{NewName: b("baz"), Tags: bs("bar1", "bar2")},
			result: false,
		},
		{
			op:     RollupOp{NewName: b("baz"), Tags: bs("bar2", "bar1")},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, rollupOp.SameTransform(input.op))
	}
}

func TestOpUnionMarshalJSON(t *testing.T) {
	inputs := []struct {
		op       OpUnion
		expected string
	}{
		{
			op: OpUnion{
				Type:        AggregationOpType,
				Aggregation: AggregationOp{Type: aggregation.Sum},
			},
			expected: `{"aggregation":"Sum"}`,
		},
		{
			op: OpUnion{
				Type:           TransformationOpType,
				Transformation: TransformationOp{Type: transformation.PerSecond},
			},
			expected: `{"transformation":"PerSecond"}`,
		},
		{
			op: OpUnion{
				Type: RollupOpType,
				Rollup: RollupOp{
					NewName:       b("testRollup"),
					Tags:          bs("tag1", "tag2"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
				},
			},
			expected: `{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":["Min","Max"]}}`,
		},
		{
			op: OpUnion{
				Type: RollupOpType,
				Rollup: RollupOp{
					NewName:       b("testRollup"),
					Tags:          bs("tag1", "tag2"),
					AggregationID: aggregation.DefaultID,
				},
			},
			expected: `{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":null}}`,
		},
	}

	for _, input := range inputs {
		b, err := json.Marshal(input.op)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestOpUnionMarshalJSONError(t *testing.T) {
	op := OpUnion{}
	_, err := json.Marshal(op)
	require.Error(t, err)
}

func TestOpUnionMarshalRoundtrip(t *testing.T) {
	ops := []OpUnion{
		{
			Type:        AggregationOpType,
			Aggregation: AggregationOp{Type: aggregation.Sum},
		},
		{
			Type:           TransformationOpType,
			Transformation: TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	}

	testmarshal.TestMarshalersRoundtrip(t, ops, []testmarshal.Marshaler{testmarshal.JSONMarshaler, testmarshal.YAMLMarshaler})
}

func TestPipelineMarshalJSON(t *testing.T) {
	p := NewPipeline([]OpUnion{
		{
			Type:        AggregationOpType,
			Aggregation: AggregationOp{Type: aggregation.Sum},
		},
		{
			Type:           TransformationOpType,
			Transformation: TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	b, err := json.Marshal(p)
	require.NoError(t, err)

	expected := `[{"aggregation":"Sum"},` +
		`{"transformation":"PerSecond"},` +
		`{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":["Min","Max"]}},` +
		`{"rollup":{"newName":"testRollup","tags":["tag1","tag2"],"aggregation":null}}]`
	require.Equal(t, expected, string(b))
}

func TestPipelineMarshalRoundtrip(t *testing.T) {
	p := NewPipeline([]OpUnion{
		{
			Type:        AggregationOpType,
			Aggregation: AggregationOp{Type: aggregation.Sum},
		},
		{
			Type:           TransformationOpType,
			Transformation: TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})

	testmarshal.TestMarshalersRoundtrip(t, []Pipeline{p}, []testmarshal.Marshaler{testmarshal.YAMLMarshaler, testmarshal.JSONMarshaler})
}

func TestPipelineUnmarshalYAML(t *testing.T) {
	input := `
- aggregation: Sum
- transformation: PerSecond
- rollup:
    newName: testRollup
    tags:
      - tag1
      - tag2
    aggregation:
      - Min
      - Max
- rollup:
    newName: testRollup2
    tags:
      - tag3
      - tag4
`

	var pipeline Pipeline
	require.NoError(t, yaml.Unmarshal([]byte(input), &pipeline))

	expected := NewPipeline([]OpUnion{
		{
			Type:        AggregationOpType,
			Aggregation: AggregationOp{Type: aggregation.Sum},
		},
		{
			Type:           TransformationOpType,
			Transformation: TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup"),
				Tags:          bs("tag1", "tag2"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			},
		},
		{
			Type: RollupOpType,
			Rollup: RollupOp{
				NewName:       b("testRollup2"),
				Tags:          bs("tag3", "tag4"),
				AggregationID: aggregation.DefaultID,
			},
		},
	})
	require.Equal(t, expected, pipeline)
}

func b(v string) []byte       { return []byte(v) }
func bs(v ...string) [][]byte { return bytes.ArraysFromStringArray(v) }
