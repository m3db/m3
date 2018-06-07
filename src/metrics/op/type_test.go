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

package op

import (
	"testing"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/transformation"

	"github.com/stretchr/testify/require"
)

func TestAggregationEqual(t *testing.T) {
	inputs := []struct {
		a1       Aggregation
		a2       Aggregation
		expected bool
	}{
		{
			a1:       Aggregation{aggregation.Count},
			a2:       Aggregation{aggregation.Count},
			expected: true,
		},
		{
			a1:       Aggregation{aggregation.Count},
			a2:       Aggregation{aggregation.Sum},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestTransformationEqual(t *testing.T) {
	inputs := []struct {
		a1       Transformation
		a2       Transformation
		expected bool
	}{
		{
			a1:       Transformation{transformation.Absolute},
			a2:       Transformation{transformation.Absolute},
			expected: true,
		},
		{
			a1:       Transformation{transformation.Absolute},
			a2:       Transformation{transformation.PerSecond},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a1.Equal(input.a2))
		require.Equal(t, input.expected, input.a2.Equal(input.a1))
	}
}

func TestTransformationClone(t *testing.T) {
	source := Transformation{transformation.Absolute}
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
				Operations: []Union{
					{
						Type:        AggregationType,
						Aggregation: Aggregation{Type: aggregation.Last},
					},
					{
						Type:           TransformationType,
						Transformation: Transformation{Type: transformation.PerSecond},
					},
					{
						Type: RollupType,
						Rollup: Rollup{
							NewName:         b("foo"),
							Tags:            [][]byte{b("tag1"), b("tag2")},
							AggregationType: aggregation.Sum,
						},
					},
				},
			},
			expected: "{operations: [{aggregation: Last}, {transformation: PerSecond}, {rollup: {name: foo, tags: [tag1, tag2], aggregation: Sum}}]}",
		},
		{
			p: Pipeline{
				Operations: []Union{
					{
						Type: Type(10),
					},
				},
			},
			expected: "{operations: [{unknown op type: Type(10)}]}",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func b(str string) []byte { return []byte(str) }
