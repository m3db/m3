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

package native

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/json"
	xerrors "github.com/m3db/m3/src/x/errors"
	xjson "github.com/m3db/m3/src/x/json"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	promQuery = `http_requests_total{job="prometheus",group="canary"}`
)

func defaultParams() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(queryParam, promQuery)
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, string(now.Add(time.Hour).Format(time.RFC3339)))
	vals.Add(handleroptions.StepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func testParseParams(req *http.Request) (models.RequestParams, error) {
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	if err != nil {
		return models.RequestParams{}, err
	}

	_, fetchOpts, err := fetchOptsBuilder.NewFetchOptions(req.Context(), req)
	if err != nil {
		return models.RequestParams{}, err
	}

	return parseParams(req, executor.NewEngineOptions(), fetchOpts)
}

func TestParamParsing(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	r, err := testParseParams(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestParamParsing_POST(t *testing.T) {
	params := defaultParams().Encode()
	req := httptest.NewRequest("POST", PromReadURL, strings.NewReader(params))
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeFormURLEncoded)

	r, err := testParseParams(req)
	require.NoError(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInstantaneousParamParsing(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	params := url.Values{}
	now := time.Now()
	params.Add(queryParam, promQuery)
	params.Add(timeParam, now.Format(time.RFC3339))
	req.URL.RawQuery = params.Encode()
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 10 * time.Second,
		})
	require.NoError(t, err)
	_, fetchOpts, err := fetchOptsBuilder.NewFetchOptions(req.Context(), req)
	require.NoError(t, err)

	r, err := parseInstantaneousParams(req, executor.NewEngineOptions(),
		fetchOpts)
	require.NoError(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInvalidStart(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(startParam)
	req.URL.RawQuery = vals.Encode()
	_, err := testParseParams(req)
	require.NotNil(t, err, "unable to parse request")
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestInvalidTarget(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(queryParam)
	req.URL.RawQuery = vals.Encode()

	p, err := testParseParams(req)
	require.NotNil(t, err, "unable to parse request")
	assert.NotNil(t, p.Start)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestParseBlockType(t *testing.T) {
	for _, test := range []struct {
		input    string
		expected models.FetchedBlockType
		err      bool
	}{
		{
			input:    "0",
			expected: models.TypeSingleBlock,
		},
		{
			input: "1",
			err:   true,
		},
		{
			input: "2",
			err:   true,
		},
		{
			input: "foo",
			err:   true,
		},
	} {
		t.Run(test.input, func(t *testing.T) {
			req := httptest.NewRequest("GET", PromReadURL, nil)
			p := defaultParams()
			p.Set("block-type", test.input)
			req.URL.RawQuery = p.Encode()

			r, err := testParseParams(req)
			if !test.err {
				require.NoError(t, err, "should be no block error")
				require.Equal(t, test.expected, r.BlockType)
			} else {
				require.Error(t, err, "should be block error")
			}
		})
	}
}

func TestRenderResultsJSON(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	series := testSeries(2)

	start := series[0].Values().DatapointAt(0).Timestamp
	params := models.RequestParams{
		Start: start,
		End:   start.Add(time.Hour * 1),
	}

	readResult := ReadResult{Series: series}
	rr := RenderResultsJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: true,
	})
	require.Equal(t, false, rr.LimitedMaxReturnedData)
	require.Equal(t, 6, rr.Datapoints)
	require.Equal(t, 3, rr.Series)
	require.Equal(t, 3, rr.TotalSeries)

	rr = RenderResultsJSON(jw, readResult, RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: true,
	})
	require.Equal(t, false, rr.LimitedMaxReturnedData)
	require.Equal(t, 6, rr.Datapoints)
	require.Equal(t, 3, rr.Series)
	require.Equal(t, 3, rr.TotalSeries)
	require.NoError(t, jw.Close())

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

	actual := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderResultsJSONWithDroppedNaNs(t *testing.T) {
	var (
		start       = time.Unix(1535948880, 0)
		buffer      = bytes.NewBuffer(nil)
		step        = 10 * time.Second
		valsWithNaN = ts.NewFixedStepValues(step, 2, 1, start)
		params      = models.RequestParams{
			Start: start,
			End:   start.Add(2 * step),
		}
	)

	jw := json.NewWriter(buffer)

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
	readResult := ReadResult{
		Series: series,
		Meta:   meta,
	}

	// Ensure idempotent by running first once with noop render.
	rr := RenderResultsJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: false,
	})
	require.Equal(t, false, rr.LimitedMaxReturnedData)
	require.Equal(t, 3, rr.Datapoints)
	require.Equal(t, 2, rr.Series)
	require.Equal(t, 3, rr.TotalSeries)

	rr = RenderResultsJSON(jw, readResult, RenderResultsOptions{
		Start:    params.Start,
		End:      params.End,
		KeepNaNs: false,
	})
	require.Equal(t, false, rr.LimitedMaxReturnedData)
	require.Equal(t, 3, rr.Datapoints)
	require.Equal(t, 2, rr.Series)
	require.Equal(t, 3, rr.TotalSeries)
	require.NoError(t, jw.Close())

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

	actual := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
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

	readResult := ReadResult{
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

	// Ensure idempotent by running first once with noop render.
	r := RenderResultsInstantaneousJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{KeepNaNs: true})
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 3, r.Datapoints)
	require.Equal(t, 3, r.Series)
	require.Equal(t, 3, r.TotalSeries)

	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	r = RenderResultsInstantaneousJSON(jw, readResult, RenderResultsOptions{KeepNaNs: true})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 3, r.Datapoints)
	require.Equal(t, 3, r.Series)
	require.Equal(t, 3, r.TotalSeries)

	expectedWithNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{foo, nan, bar},
		},
	})
	actualWithNaN := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expectedWithNaN, actualWithNaN, xtest.Diff(expectedWithNaN, actualWithNaN))

	// Ensure idempotent by running first once with noop render.
	r = RenderResultsInstantaneousJSON(json.NewNoopWriter(),
		readResult,
		RenderResultsOptions{KeepNaNs: false})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 2, r.Datapoints)
	require.Equal(t, 2, r.Series)
	require.Equal(t, 3, r.TotalSeries) // This is > rendered series due to keepNaN: false.

	buffer = bytes.NewBuffer(nil)
	jw = json.NewWriter(buffer)
	r = RenderResultsInstantaneousJSON(jw, readResult, RenderResultsOptions{KeepNaNs: false})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 2, r.Datapoints)
	require.Equal(t, 2, r.Series)
	require.Equal(t, 3, r.TotalSeries) // This is > rendered series due to keepNaN: false.
	expectedWithoutNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{foo, bar},
		},
	})
	actualWithoutNaN := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expectedWithoutNaN, actualWithoutNaN, xtest.Diff(expectedWithoutNaN, actualWithoutNaN))
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

	readResult := ReadResult{
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

	// Ensure idempotent by running first once with noop render.
	r := RenderResultsInstantaneousJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{KeepNaNs: true})
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 2, r.Datapoints)
	require.Equal(t, 2, r.Series)
	require.Equal(t, 2, r.TotalSeries)

	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	r = RenderResultsInstantaneousJSON(jw, readResult, RenderResultsOptions{KeepNaNs: true})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 2, r.Datapoints)
	require.Equal(t, 2, r.Series)
	require.Equal(t, 2, r.TotalSeries)

	expectedWithNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{nan1, nan2},
		},
	})
	actualWithNaN := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expectedWithNaN, actualWithNaN, xtest.Diff(expectedWithNaN, actualWithNaN))

	// Ensure idempotent by running first once with noop render.
	r = RenderResultsInstantaneousJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{KeepNaNs: false})
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 0, r.Datapoints)
	require.Equal(t, 0, r.Series)
	require.Equal(t, 2, r.TotalSeries) // This is > rendered series due to keepNaN: false.

	buffer = bytes.NewBuffer(nil)
	jw = json.NewWriter(buffer)
	r = RenderResultsInstantaneousJSON(jw, readResult, RenderResultsOptions{KeepNaNs: false})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 0, r.Datapoints)
	require.Equal(t, 0, r.Series)
	require.Equal(t, 2, r.TotalSeries) // This is > rendered series due to keepNaN: false.
	expectedWithoutNaN := xtest.MustPrettyJSONMap(t, xjson.Map{
		"status": "success",
		"data": xjson.Map{
			"resultType": "vector",
			"result":     xjson.Array{},
		},
	})
	actualWithoutNaN := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expectedWithoutNaN, actualWithoutNaN, xtest.Diff(expectedWithoutNaN, actualWithoutNaN))
}

func TestRenderInstantaneousResultsJSONScalar(t *testing.T) {
	start := time.Unix(1535948880, 0)

	series := []*ts.Series{
		ts.NewSeries(
			[]byte("foo"),
			ts.NewFixedStepValues(10*time.Second, 1, 5, start),
			test.TagSliceToTags([]models.Tag{})),
	}

	readResult := ReadResult{
		Series:    series,
		Meta:      block.NewResultMetadata(),
		BlockType: block.BlockScalar,
	}

	// Ensure idempotent by running first once with noop render.
	r := RenderResultsInstantaneousJSON(json.NewNoopWriter(), readResult, RenderResultsOptions{KeepNaNs: false})
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 1, r.Datapoints)
	require.Equal(t, 1, r.Series)
	require.Equal(t, 1, r.TotalSeries)

	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	r = RenderResultsInstantaneousJSON(jw, readResult, RenderResultsOptions{KeepNaNs: false})
	require.NoError(t, jw.Close())
	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 1, r.Datapoints)
	require.Equal(t, 1, r.Series)
	require.Equal(t, 1, r.TotalSeries)

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

	actual := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
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
			vals.EXPECT().DatapointAt(i).Return(dps[i])
		}

		vals.EXPECT().Len().Return(len(dps))
		series = append(series, ts.NewSeries([]byte(d.name), vals, tags))
	}

	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	r := RenderResultsJSON(jw,
		ReadResult{Series: series},
		RenderResultsOptions{Start: start, End: end})
	require.NoError(t, jw.Close())

	require.Equal(t, false, r.LimitedMaxReturnedData)
	require.Equal(t, 3, r.Series)
	require.Equal(t, 9, r.TotalSeries)
	require.Equal(t, 5, r.Datapoints)

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
	actual := xtest.MustPrettyJSONString(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderResultsJSONWithLimits(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	jw := json.NewWriter(buffer)
	defer require.NoError(t, jw.Close())
	series := testSeries(5)

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
			readResult := ReadResult{Series: series}
			o := RenderResultsOptions{
				Start:    params.Start,
				End:      params.End,
				KeepNaNs: true,
			}
			if test.limit != nil {
				o.ReturnedDatapointsLimit = *test.limit
			}
			r := RenderResultsJSON(jw, readResult, o)
			require.Equal(t, 3, r.TotalSeries)
			require.Equal(t, test.expectedSeries, r.Series)
			require.Equal(t, test.expectedDatapoints, r.Datapoints)
			require.Equal(t, test.expectedLimited, r.LimitedMaxReturnedData)
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
