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

package temporal

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/test/transformtest"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nan = math.NaN()

type testCase struct {
	name     string
	opType   string
	vals     [][]float64
	expected [][]float64
}

type opGenerator func(t *testing.T, tc testCase) transform.Params

func testTemporalFunc(t *testing.T, opGen opGenerator, tests []testCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, bounds := test.GenerateValuesAndBounds(tt.vals, nil)
			boundStart := bounds.Start

			seriesMetas := []block.SeriesMeta{
				{
					Name: []byte("s1"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v1"),
					}}).SetName([]byte("foobar")),
				},
				{
					Name: []byte("s2"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v2"),
					}}).SetName([]byte("foobar")),
				},
			}

			bl := test.NewUnconsolidatedBlockFromDatapointsWithMeta(models.Bounds{
				Start:    bounds.Start.Add(-2 * bounds.Duration),
				Duration: bounds.Duration * 2,
				StepSize: bounds.StepSize,
			}, seriesMetas, values)

			c, sink := executor.NewControllerWithSink(parser.NodeID(1))
			baseOp := opGen(t, tt)
			node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
				TimeSpec: transform.TimeSpec{
					Start: boundStart.Add(-2 * bounds.Duration),
					End:   bounds.End(),
					Step:  time.Second,
				},
			}))

			err := node.Process(models.NoopQueryContext(), parser.NodeID(0), bl)
			require.NoError(t, err)

			test.EqualsWithNansWithDelta(t, tt.expected, sink.Values, 0.0001)
			// Name should be dropped from series tags.
			expectedSeriesMetas := []block.SeriesMeta{
				block.SeriesMeta{
					Name: []byte("t1=v1,"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v1"),
					}}),
				},
				block.SeriesMeta{
					Name: []byte("t1=v2,"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v2"),
					}})},
			}

			assert.Equal(t, expectedSeriesMetas, sink.Metas)
		})
	}
}

func TestGetIndicesError(t *testing.T) {
	size := 10
	now := time.Now().Truncate(time.Minute)
	dps := make([]ts.Datapoint, size)
	s := int64(time.Second)
	for i := range dps {
		dps[i] = ts.Datapoint{
			Timestamp: now.Add(time.Duration(int(s) * i)),
			Value:     float64(i),
		}
	}

	l, r, ok := getIndices(dps, 0, 0, -1)
	require.Equal(t, -1, l)
	require.Equal(t, -1, r)
	require.False(t, ok)

	l, r, ok = getIndices(dps, 0, 0, size)
	require.Equal(t, -1, l)
	require.Equal(t, -1, r)
	require.False(t, ok)

	pastBound := xtime.ToUnixNano(now.Add(time.Hour))
	l, r, ok = getIndices(dps, pastBound, pastBound+10, 0)
	require.Equal(t, 0, l)
	require.Equal(t, 10, r)
	require.False(t, ok)
}
