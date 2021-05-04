// Copyright (c) 2021  Uber Technologies, Inc.
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

package read

import (
	stdjson "encoding/json"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"
)

func TestRenderResults(t *testing.T) {
	recorder := httptest.NewRecorder()
	series := testSeries(2)

	start := series[0].Values().DatapointAt(0).Timestamp
	params := models.RequestParams{
		Start: start,
		End:   start.Add(time.Hour * 1),
	}

	readResult := Result{Series: series}
	resultIter := NewM3QueryResultIterator(readResult)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: true,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  6,
		Series:      3,
		TotalSeries: 3,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expected := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"warnings": xjson.Array{
			"m3db exceeded query limit: results not exhaustive",
		},
		"data": xjson.Map{
			"resultType": "matrix",
			"result": xjson.Array{
				xjson.Map{
					"metric": xjson.Map{
						"bar": "baz",
						"qux": "qaz",
					},
					"values": xjson.Array{
						xjson.Array{
							1535948880,
							"1",
						},
						xjson.Array{
							1535948890,
							"NaN",
						},
					},
					"step_size_ms": 10000,
				},
				xjson.Map{
					"metric": xjson.Map{
						"baz": "bar",
						"qaz": "qux",
					},
					"values": xjson.Array{
						xjson.Array{
							1535948880,
							"2",
						},
						xjson.Array{
							1535948890,
							"2",
						},
					},
					"step_size_ms": 10000,
				},
				xjson.Map{
					"metric": xjson.Map{
						"biz": "baz",
						"qux": "qaz",
					},
					"values": xjson.Array{
						xjson.Array{
							1535948880,
							"NaN",
						},
						xjson.Array{
							1535948890,
							"NaN",
						},
					},
					"step_size_ms": 10000,
				},
			},
		},
	})

	actual := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderResultsJSONWithDroppedNaNs(t *testing.T) {
	var (
		start       = time.Unix(1535948880, 0)
		recorder    = httptest.NewRecorder()
		step        = 10 * time.Second
		valsWithNaN = ts.NewFixedStepValues(step, 2, 1, start)
		params      = models.RequestParams{
			Start: start,
			End:   start.Add(2 * step),
		}
	)

	valsWithNaN.SetValueAt(1, math.NaN())
	series := []*ts.Series{
		ts.NewSeries([]byte("foo"),
			valsWithNaN, test.TagSliceToTags([]models.Tag{
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("foobar"),
			ts.NewFixedStepValues(step, 2, math.NaN(), start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("biz"), Value: []byte("baz")},
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(step, 2, 2, start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
				{Name: []byte("qaz"), Value: []byte("qux")},
			})),
	}

	meta := block.NewResultMetadata()
	meta.AddWarning("foo", "bar")
	meta.AddWarning("baz", "qux")
	readResult := Result{
		Series: series,
		Meta:   meta,
	}

	resultIter := NewM3QueryResultIterator(readResult)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: false,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  3,
		Series:      2,
		TotalSeries: 3,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expected := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"warnings": xjson.Array{
			"foo_bar",
			"baz_qux",
		},
		"data": xjson.Map{
			"resultType": "matrix",
			"result": xjson.Array{
				xjson.Map{
					"metric": xjson.Map{
						"bar": "baz",
						"qux": "qaz",
					},
					"values": xjson.Array{
						xjson.Array{
							1535948880,
							"1",
						},
					},
					"step_size_ms": 10000,
				},
				xjson.Map{
					"metric": xjson.Map{
						"baz": "bar",
						"qaz": "qux",
					},
					"values": xjson.Array{
						xjson.Array{
							1535948880,
							"2",
						},
						xjson.Array{
							1535948890,
							"2",
						},
					},
					"step_size_ms": 10000,
				},
			},
		},
	})

	actual := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderInstantaneousResultsJSONVector(t *testing.T) {
	start := time.Unix(1535948880, 0)

	series := []*ts.Series{
		ts.NewSeries([]byte("foo"),
			ts.NewFixedStepValues(10*time.Second, 1, 1, start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("nan"),
			ts.NewFixedStepValues(10*time.Second, 1, math.NaN(), start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(10*time.Second, 1, 2, start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
				{Name: []byte("qaz"), Value: []byte("qux")},
			})),
	}

	readResult := Result{
		Series: series,
		Meta:   block.NewResultMetadata(),
	}

	foo := xjson.Map{
		"metric": xjson.Map{
			"bar": "baz",
			"qux": "qaz",
		},
		"value": xjson.Array{
			1535948880,
			"1",
		},
	}

	bar := xjson.Map{
		"metric": xjson.Map{
			"baz": "bar",
			"qaz": "qux",
		},
		"value": xjson.Array{
			1535948880,
			"2",
		},
	}

	nan := xjson.Map{
		"metric": xjson.Map{
			"baz": "bar",
		},
		"value": xjson.Array{
			1535948880,
			"NaN",
		},
	}

	// Response that keeps NaNs
	recorder := httptest.NewRecorder()
	resultIter := NewM3QueryResultIterator(readResult)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		KeepNaNs: true,
		Instant:  true,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  3,
		Series:      3,
		TotalSeries: 3,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expectedWithNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{foo, nan, bar},
		},
	})
	actualWithNaN := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expectedWithNaN, actualWithNaN, xtest.Diff(expectedWithNaN, actualWithNaN))

	// Response that drops NaNs
	recorder = httptest.NewRecorder()
	resultIter = NewM3QueryResultIterator(readResult)
	metrics = NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Instant: true,
	})
	result = handleroptions.ReturnedDataLimited{
		Datapoints:  2,
		Series:      2,
		TotalSeries: 3,
		Limited:     false,
	}
	value, err = stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expectedWithoutNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{foo, bar},
		},
	})
	actualWithoutNaN := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expectedWithoutNaN, actualWithoutNaN, xtest.Diff(expectedWithoutNaN, actualWithoutNaN))
}

func TestRenderInstantaneousResultsNansOnlyJSON(t *testing.T) {
	start := time.Unix(1535948880, 0)

	series := []*ts.Series{
		ts.NewSeries([]byte("nan"),
			ts.NewFixedStepValues(10*time.Second, 1, math.NaN(), start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("nan"),
			ts.NewFixedStepValues(10*time.Second, 1, math.NaN(), start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
			})),
	}

	readResult := Result{
		Series: series,
		Meta:   block.NewResultMetadata(),
	}

	nan1 := xjson.Map{
		"metric": xjson.Map{
			"qux": "qaz",
		},
		"value": xjson.Array{
			1535948880,
			"NaN",
		},
	}

	nan2 := xjson.Map{
		"metric": xjson.Map{
			"baz": "bar",
		},
		"value": xjson.Array{
			1535948880,
			"NaN",
		},
	}

	// Response that keeps NaNs
	recorder := httptest.NewRecorder()
	resultIter := NewM3QueryResultIterator(readResult)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		KeepNaNs: true,
		Instant:  true,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  2,
		Series:      2,
		TotalSeries: 2,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expectedWithNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{nan1, nan2},
		},
	})
	actualWithNaN := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expectedWithNaN, actualWithNaN, xtest.Diff(expectedWithNaN, actualWithNaN))

	// Response that drops NaNs
	recorder = httptest.NewRecorder()
	resultIter = NewM3QueryResultIterator(readResult)
	metrics = NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Instant: true,
	})
	result = handleroptions.ReturnedDataLimited{
		Datapoints:  0,
		Series:      0,
		TotalSeries: 2,
		Limited:     false,
	}
	value, err = stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expectedWithoutNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{},
		},
	})
	actualWithoutNaN := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expectedWithoutNaN, actualWithoutNaN, xtest.Diff(expectedWithoutNaN, actualWithoutNaN))
}

func TestRenderInstantaneousResultsJSONScalar(t *testing.T) {
	start := time.Unix(1535948880, 0)

	series := []*ts.Series{
		ts.NewSeries(
			[]byte("foo"),
			ts.NewFixedStepValues(10*time.Second, 1, 5, start),
			test.TagSliceToTags([]models.Tag{})),
	}

	readResult := Result{
		Series:    series,
		Meta:      block.NewResultMetadata(),
		BlockType: block.BlockScalar,
	}

	recorder := httptest.NewRecorder()
	resultIter := NewM3QueryResultIterator(readResult)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Instant: true,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  1,
		Series:      1,
		TotalSeries: 1,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expected := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "scalar",
			"result": xjson.Array{
				1535948880,
				"5",
			},
		},
	})

	actual := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestSanitizeSeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nan := math.NaN()
	testData := []struct {
		name string
		data []float64
	}{
		{"1", []float64{nan, nan, nan, nan}},
		{"2", []float64{nan, nan, nan, 1}},
		{"3", []float64{nan, nan, nan, nan}},
		{"4", []float64{nan, nan, 1, nan}},
		{"5", []float64{1, 1, 1, 1}},
		{"6", []float64{nan, nan, nan, nan}},
		{"no values", []float64{}},
		{"non-nan point is too early", []float64{1, nan, nan, nan, nan}},
		{"non-nan point is too late ", []float64{nan, nan, nan, nan, 1}},
	}

	var (
		series = make([]*ts.Series, 0, len(testData))
		tags   = models.NewTags(0, models.NewTagOptions())
		now    = time.Unix(1535948880, 0)
		step   = time.Minute
		start  = now.Add(step)
		end    = now.Add(step * 3)
	)

	for _, d := range testData {
		vals := ts.NewMockValues(ctrl)
		dps := make(ts.Datapoints, 0, len(d.data))
		for i, p := range d.data {
			timestamp := now.Add(time.Duration(i) * step)
			dps = append(dps, ts.Datapoint{Value: p, Timestamp: timestamp})
			vals.EXPECT().DatapointAt(i).Return(dps[i]).AnyTimes()
		}

		vals.EXPECT().Len().Return(len(dps)).AnyTimes()
		series = append(series, ts.NewSeries([]byte(d.name), vals, tags))
	}

	recorder := httptest.NewRecorder()
	resultIter := NewM3QueryResultIterator(Result{Series: series})
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())
	RenderResults(recorder, resultIter, metrics, zap.NewNop(), RenderResultsOptions{
		Start: start,
		End:   end,
	})
	result := handleroptions.ReturnedDataLimited{
		Datapoints:  5,
		Series:      3,
		TotalSeries: 9,
		Limited:     false,
	}
	value, err := stdjson.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	expected := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "matrix",
			"result": xjson.Array{
				xjson.Map{
					"metric": xjson.Map{},
					"values": xjson.Array{
						xjson.Array{
							1535949060,
							"1",
						},
					},
				},
				xjson.Map{
					"metric": xjson.Map{},
					"values": xjson.Array{
						xjson.Array{
							1535949000,
							"1",
						},
					},
				},
				xjson.Map{
					"metric": xjson.Map{},
					"values": xjson.Array{
						xjson.Array{
							1535948940,
							"1",
						},
						xjson.Array{
							1535949000,
							"1",
						},
						xjson.Array{
							1535949060,
							"1",
						},
					},
				},
			},
		},
		"warnings": xjson.Array{
			"m3db exceeded query limit: results not exhaustive",
		},
	})
	actual := xtest.MustPrettyJSONString(t, recorder.Body.String())
	require.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderResultsJSONWithLimits(t *testing.T) {
	series := testSeries(5)
	metrics := NewReturnedDataMetrics(instrument.NewOptions().MetricsScope())

	start := series[0].Values().DatapointAt(0).Timestamp
	params := models.RequestParams{
		Start: start,
		End:   start.Add(time.Hour * 24),
	}

	intPrt := func(v int) *int {
		return &v
	}

	tests := []struct {
		name               string
		limit              *int
		expectedDatapoints int
		expectedSeries     int
		expectedLimited    bool
	}{
		{
			name:               "Omit limit",
			expectedDatapoints: 15,
			expectedSeries:     3,
			expectedLimited:    false,
		},
		{
			name:               "Below limit",
			limit:              intPrt(16),
			expectedDatapoints: 15,
			expectedSeries:     3,
			expectedLimited:    false,
		},
		{
			name:               "At limit",
			limit:              intPrt(15),
			expectedDatapoints: 15,
			expectedSeries:     3,
			expectedLimited:    false,
		},
		{
			name:               "Above limit - skip 1 series high",
			limit:              intPrt(14),
			expectedDatapoints: 10,
			expectedSeries:     2,
			expectedLimited:    true,
		},
		{
			name:               "Above limit - skip 1 series low",
			limit:              intPrt(11),
			expectedDatapoints: 10,
			expectedSeries:     2,
			expectedLimited:    true,
		},
		{
			name:               "Above limit - skip 1 series equal",
			limit:              intPrt(10),
			expectedDatapoints: 10,
			expectedSeries:     2,
			expectedLimited:    true,
		},
		{
			name:               "Above limit - skip 2 series",
			limit:              intPrt(9),
			expectedDatapoints: 5,
			expectedSeries:     1,
			expectedLimited:    true,
		},
		{
			name:               "Above limit - skip 3 series",
			limit:              intPrt(4),
			expectedDatapoints: 0,
			expectedSeries:     0,
			expectedLimited:    true,
		},
		{
			name:               "Zero enforces no limit",
			limit:              intPrt(0),
			expectedDatapoints: 15,
			expectedSeries:     3,
			expectedLimited:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			readResult := Result{Series: series}
			resultIter := NewM3QueryResultIterator(readResult)
			o := RenderResultsOptions{
				Start:    params.Start,
				End:      params.End,
				KeepNaNs: true,
			}
			if test.limit != nil {
				o.ReturnedDatapointsLimit = *test.limit
			}
			RenderResults(recorder, resultIter, metrics, zap.NewNop(), o)
			result := handleroptions.ReturnedDataLimited{
				Datapoints:  test.expectedDatapoints,
				Series:      test.expectedSeries,
				TotalSeries: 3,
				Limited:     test.expectedLimited,
			}
			value, err := stdjson.Marshal(result)
			require.NoError(t, err)
			require.Equal(t, string(value), recorder.Header().Get(headers.ReturnedDataLimitedHeader))
		})
	}
}

func testSeries(datapointsPerSeries int) []*ts.Series {
	start := time.Unix(1535948880, 0)
	valsWithNaN := ts.NewFixedStepValues(10*time.Second, datapointsPerSeries, 1, start)
	valsWithNaN.SetValueAt(1, math.NaN())
	return []*ts.Series{
		ts.NewSeries([]byte("foo"),
			valsWithNaN, test.TagSliceToTags([]models.Tag{
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(10*time.Second, datapointsPerSeries, 2, start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
				{Name: []byte("qaz"), Value: []byte("qux")},
			})),
		ts.NewSeries([]byte("foobar"),
			ts.NewFixedStepValues(10*time.Second, datapointsPerSeries, math.NaN(), start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("biz"), Value: []byte("baz")},
				{Name: []byte("qux"), Value: []byte("qaz")},
			})),
	}
}
