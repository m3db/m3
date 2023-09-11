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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/test/transformtest"
	"github.com/m3db/m3/src/query/ts"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nan = math.NaN()

type testCase struct {
	name        string
	opType      string
	vals        [][]float64
	expected    [][]float64
	withWarning bool
}

type opGenerator func(t *testing.T, tc testCase) transform.Params

const expectedWarning = "resolution larger than query range_" +
	"range: 1m0s, resolutions: 1h0m0s, 1m1s"

func buildMetadata() block.ResultMetadata {
	resultMeta := block.NewResultMetadata()
	resultMeta.Resolutions = []time.Duration{time.Second, time.Minute}

	return resultMeta
}

func buildWarningMetadata() block.ResultMetadata {
	resultMeta := buildMetadata()
	resultMeta.Resolutions = append(resultMeta.Resolutions,
		time.Second*61, time.Hour)
	return resultMeta
}

func verifyResultMetadata(t *testing.T, m block.ResultMetadata, exWarn bool) {
	warnings := m.WarningStrings()
	if !exWarn {
		assert.Equal(t, 0, len(warnings))
		return
	}

	require.Equal(t, 1, len(warnings))
	assert.Equal(t, expectedWarning, warnings[0])
}

func testTemporalFunc(t *testing.T, opGen opGenerator, tests []testCase) {
	for _, tt := range tests {
		for _, runBatched := range []bool{true, false} {
			name := tt.name + "_unbatched"
			if runBatched {
				name = tt.name + "_batched"
			}
			t.Run(name, func(t *testing.T) {
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

				resultMeta := buildMetadata()
				if tt.withWarning {
					resultMeta = buildWarningMetadata()
				}

				bl := test.NewUnconsolidatedBlockFromDatapointsWithMeta(models.Bounds{
					Start:    bounds.Start.Add(-2 * bounds.Duration),
					Duration: bounds.Duration * 2,
					StepSize: bounds.StepSize,
				}, seriesMetas, resultMeta, values, runBatched)

				c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
				baseOp := opGen(t, tt)
				node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
					TimeSpec: transform.TimeSpec{
						Start: boundStart.Add(-2 * bounds.Duration),
						End:   bounds.End(),
						Step:  time.Second,
					},
				}))

				err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
				require.NoError(t, err)

				compare.EqualsWithNansWithDelta(t, tt.expected, sink.Values, 0.0001)
				metaOne := block.SeriesMeta{
					Name: []byte("{t1=\"v1\"}"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v1"),
					}}),
				}

				metaTwo := block.SeriesMeta{
					Name: []byte("{t1=\"v2\"}"),
					Tags: models.EmptyTags().AddTags([]models.Tag{{
						Name:  []byte("t1"),
						Value: []byte("v2"),
					}})}

				// The last_over_time function acts like offset;
				// thus, it should keep the metric name.
				// For all other functions,
				// name should be dropped from series tags,
				// and the name should be the updated ID.
				var expectedSeriesMetas []block.SeriesMeta
				if tt.opType != LastType {
					expectedSeriesMetas = []block.SeriesMeta{metaOne, metaTwo}
				} else {
					expectedSeriesMetas = seriesMetas
				}
				require.Equal(t, expectedSeriesMetas, sink.Metas)
			})
		}
	}
}

func TestGetIndicesError(t *testing.T) {
	size := 10
	now := xtime.Now().Truncate(time.Minute)
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

	pastBound := now.Add(time.Hour)
	l, r, ok = getIndices(dps, pastBound, pastBound+10, 0)
	require.Equal(t, 0, l)
	require.Equal(t, 10, r)
	require.False(t, ok)
}

var _ block.SeriesIter = (*dummySeriesIter)(nil)

type dummySeriesIter struct {
	metas []block.SeriesMeta
	vals  []float64
	idx   int
}

func (it *dummySeriesIter) SeriesMeta() []block.SeriesMeta {
	return it.metas
}

func (it *dummySeriesIter) SeriesCount() int {
	return len(it.metas)
}

func (it *dummySeriesIter) Current() block.UnconsolidatedSeries {
	return block.NewUnconsolidatedSeries(
		ts.Datapoints{ts.Datapoint{Value: it.vals[it.idx]}},
		it.metas[it.idx],
		block.UnconsolidatedSeriesStats{},
	)
}

func (it *dummySeriesIter) Next() bool {
	if it.idx >= len(it.metas)-1 {
		return false
	}

	it.idx++
	return true
}

func (it *dummySeriesIter) Err() error {
	return nil
}

func (it *dummySeriesIter) Close() {
	//no-op
}

func TestParallelProcess(t *testing.T) {
	t.Run("no expected warning", func(t *testing.T) { testParallelProcess(t, false) })
	t.Run("expected warning", func(t *testing.T) { testParallelProcess(t, true) })
}

func testParallelProcess(t *testing.T, warning bool) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	tagName := "tag"
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	aggProcess := aggProcessor{
		aggFunc: func(fs []float64) float64 {
			require.Equal(t, 1, len(fs))
			return fs[0]
		},
	}

	node := baseNode{
		controller:    c,
		op:            baseOp{duration: time.Minute},
		makeProcessor: aggProcess,
		transformOpts: transform.Options{},
	}

	stepSize := time.Minute
	bl := block.NewMockBlock(ctrl)
	resultMeta := buildMetadata()
	if warning {
		resultMeta = buildWarningMetadata()
	}

	bl.EXPECT().Meta().Return(block.Metadata{
		ResultMetadata: resultMeta,
		Bounds: models.Bounds{
			StepSize: stepSize,
			Duration: stepSize,
		}}).AnyTimes()

	numSeries := 10
	seriesMetas := make([]block.SeriesMeta, 0, numSeries)
	vals := make([]float64, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		number := fmt.Sprint(i)
		name := []byte(fmt.Sprintf("%d_should_not_appear_after_func_applied", i))
		meta := block.SeriesMeta{
			Name: []byte(number),
			Tags: models.MustMakeTags(tagName, number).SetName(name),
		}

		seriesMetas = append(seriesMetas, meta)
		vals = append(vals, float64(i))
	}

	fullIter := &dummySeriesIter{
		idx:   -1,
		vals:  vals,
		metas: seriesMetas,
	}

	bl.EXPECT().SeriesIter().Return(fullIter, nil).MaxTimes(1)

	numBatches := 3
	blockMetas := make([][]block.SeriesMeta, 0, numBatches)
	blockVals := make([][]float64, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		l := numSeries/numBatches + 1
		blockMetas = append(blockMetas, make([]block.SeriesMeta, 0, l))
		blockVals = append(blockVals, make([]float64, 0, l))
	}

	for i, meta := range seriesMetas {
		idx := i % numBatches
		blockMetas[idx] = append(blockMetas[idx], meta)
		blockVals[idx] = append(blockVals[idx], float64(i))
	}

	batches := make([]block.SeriesIterBatch, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		iter := &dummySeriesIter{
			idx:   -1,
			vals:  blockVals[i],
			metas: blockMetas[i],
		}

		batches = append(batches, block.SeriesIterBatch{
			Iter: iter,
			Size: len(blockVals[i]),
		})
	}

	bl.EXPECT().MultiSeriesIter(gomock.Any()).Return(batches, nil).MaxTimes(1)
	bl.EXPECT().Close().Times(1)

	err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)

	expected := []float64{
		0, 3, 6, 9,
		1, 4, 7,
		2, 5, 8,
	}

	for i, v := range sink.Values {
		assert.Equal(t, expected[i], v[0])
	}

	for i, m := range sink.Metas {
		expected := fmt.Sprint(expected[i])
		expectedName := fmt.Sprintf("{tag=\"%s\"}", expected)
		assert.Equal(t, expectedName, string(m.Name))
		require.Equal(t, 1, m.Tags.Len())
		tag, found := m.Tags.Get([]byte(tagName))
		require.True(t, found)
		assert.Equal(t, expected, string(tag))
	}

	verifyResultMetadata(t, sink.Meta.ResultMetadata, warning)
}
