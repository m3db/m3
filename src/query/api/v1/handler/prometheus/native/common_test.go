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
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	promQuery = `http_requests_total{job="prometheus",group="canary"}`
)

var (
	timeoutOpts = &prometheus.TimeoutOpts{
		FetchTimeout: 15 * time.Second,
	}
)

func defaultParams() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(queryParam, promQuery)
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, string(now.Add(time.Hour).Format(time.RFC3339)))
	vals.Add(handler.StepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func testParseParams(req *http.Request) (models.RequestParams, *xhttp.ParseError) {
	fetchOpts, err := handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}).
		NewFetchOptions(req)
	if err != nil {
		return models.RequestParams{}, err
	}

	return parseParams(req, executor.NewEngineOptions(), timeoutOpts,
		fetchOpts, instrument.NewOptions())
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
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	r, err := testParseParams(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInstantaneousParamParsing(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	params := url.Values{}
	now := time.Now()
	params.Add(queryParam, promQuery)
	params.Add(timeParam, now.Format(time.RFC3339))
	req.URL.RawQuery = params.Encode()

	r, err := parseInstantaneousParams(req, executor.NewEngineOptions(),
		timeoutOpts, storage.NewFetchOptions(), instrument.NewOptions())
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInvalidStart(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(startParam)
	req.URL.RawQuery = vals.Encode()
	_, err := testParseParams(req)
	require.NotNil(t, err, "unable to parse request")
	require.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestInvalidTarget(t *testing.T) {
	req := httptest.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(queryParam)
	req.URL.RawQuery = vals.Encode()

	p, err := testParseParams(req)
	require.NotNil(t, err, "unable to parse request")
	assert.NotNil(t, p.Start)
	require.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestParseBlockType(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/foo", nil)
	assert.Equal(t, models.TypeSingleBlock, parseBlockType(r,
		instrument.NewOptions()))

	r = httptest.NewRequest(http.MethodGet, "/foo?block-type=0", nil)
	assert.Equal(t, models.TypeSingleBlock, parseBlockType(r,
		instrument.NewOptions()))

	r = httptest.NewRequest(http.MethodGet, "/foo?block-type=1", nil)
	assert.Equal(t, models.TypeMultiBlock, parseBlockType(r,
		instrument.NewOptions()))

	r = httptest.NewRequest(http.MethodGet, "/foo?block-type=2", nil)
	assert.Equal(t, models.TypeDecodedBlock, parseBlockType(r,
		instrument.NewOptions()))

	r = httptest.NewRequest(http.MethodGet, "/foo?block-type=3", nil)
	assert.Equal(t, models.TypeSingleBlock, parseBlockType(r,
		instrument.NewOptions()))

	r = httptest.NewRequest(http.MethodGet, "/foo?block-type=bar", nil)
	assert.Equal(t, models.TypeSingleBlock, parseBlockType(r,
		instrument.NewOptions()))
}

func TestRenderResultsJSON(t *testing.T) {
	start := time.Unix(1535948880, 0)
	buffer := bytes.NewBuffer(nil)
	params := models.RequestParams{}
	valsWithNaN := ts.NewFixedStepValues(10*time.Second, 2, 1, start)
	valsWithNaN.SetValueAt(1, math.NaN())

	series := []*ts.Series{
		ts.NewSeries([]byte("foo"),
			valsWithNaN, test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("bar"), Value: []byte("baz")},
				models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(10*time.Second, 2, 2, start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("baz"), Value: []byte("bar")},
				models.Tag{Name: []byte("qaz"), Value: []byte("qux")},
			})),
		ts.NewSeries([]byte("foobar"),
			ts.NewFixedStepValues(10*time.Second, 2, math.NaN(), start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("biz"), Value: []byte("baz")},
				models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
			})),
	}

	renderResultsJSON(buffer, series, params, true)

	expected := mustPrettyJSON(t, `
	{
		"status": "success",
		"data": {
			"resultType": "matrix",
			"result": [
				{
					"metric": {
						"bar": "baz",
						"qux": "qaz"
					},
					"values": [
						[
							1535948880,
							"1"
						],
						[
							1535948890,
							"NaN"
						]
					],
					"step_size_ms": 10000
				},
				{
					"metric": {
						"baz": "bar",
						"qaz": "qux"
					},
					"values": [
						[
							1535948880,
							"2"
						],
						[
							1535948890,
							"2"
						]
					],
					"step_size_ms": 10000
				},
				{
					"metric": {
						"biz": "baz",
						"qux": "qaz"
					},
					"values": [
						[
							1535948880,
							"NaN"
						],
						[
							1535948890,
							"NaN"
						]
					],
					"step_size_ms": 10000
				}
			]
		}
	}
	`)

	actual := mustPrettyJSON(t, buffer.String())
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

	valsWithNaN.SetValueAt(1, math.NaN())
	series := []*ts.Series{
		ts.NewSeries([]byte("foo"),
			valsWithNaN, test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("bar"), Value: []byte("baz")},
				models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(step, 2, 2, start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("baz"), Value: []byte("bar")},
				models.Tag{Name: []byte("qaz"), Value: []byte("qux")},
			})),
		ts.NewSeries([]byte("foobar"),
			ts.NewFixedStepValues(step, 2, math.NaN(), start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("biz"), Value: []byte("baz")},
				models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
			})),
	}

	renderResultsJSON(buffer, series, params, false)

	expected := mustPrettyJSON(t, `
	{
		"status": "success",
		"data": {
			"resultType": "matrix",
			"result": [
				{
					"metric": {
						"bar": "baz",
						"qux": "qaz"
					},
					"values": [
						[
							1535948880,
							"1"
						]
					],
					"step_size_ms": 10000
				},
				{
					"metric": {
						"baz": "bar",
						"qaz": "qux"
					},
					"values": [
						[
							1535948880,
							"2"
						],
						[
							1535948890,
							"2"
						]
					],
					"step_size_ms": 10000
				}
			]
		}
	}
	`)

	actual := mustPrettyJSON(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestRenderInstantaneousResultsJSON(t *testing.T) {
	start := time.Unix(1535948880, 0)
	buffer := bytes.NewBuffer(nil)
	series := []*ts.Series{
		ts.NewSeries([]byte("foo"),
			ts.NewFixedStepValues(10*time.Second, 1, 1, start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("bar"), Value: []byte("baz")},
				models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
			})),
		ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(10*time.Second, 1, 2, start), test.TagSliceToTags([]models.Tag{
				models.Tag{Name: []byte("baz"), Value: []byte("bar")},
				models.Tag{Name: []byte("qaz"), Value: []byte("qux")},
			})),
	}

	renderResultsInstantaneousJSON(buffer, series)

	expected := mustPrettyJSON(t, `
	{
		"status": "success",
		"data": {
			"resultType": "vector",
			"result": [
				{
					"metric": {
						"bar": "baz",
						"qux": "qaz"
					},
					"value": [
						1535948880,
						"1"
					]
				},
				{
					"metric": {
						"baz": "bar",
						"qaz": "qux"
					},
					"value": [
						1535948880,
						"2"
					]
 				}
			]
		}
	}
	`)
	actual := mustPrettyJSON(t, buffer.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func mustPrettyJSON(t *testing.T, str string) string {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal([]byte(str), &unmarshalled)
	require.NoError(t, err)
	pretty, err := json.MarshalIndent(unmarshalled, "", "  ")
	require.NoError(t, err)
	return string(pretty)
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
		now    = time.Now()
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
		}

		vals.EXPECT().Datapoints().Return(dps)
		series = append(series, ts.NewSeries([]byte(d.name), vals, tags))
	}

	series = filterNaNSeries(series, start, end)
	require.Equal(t, 3, len(series))
	assert.Equal(t, "2", string(series[0].Name()))
	assert.Equal(t, "4", string(series[1].Name()))
	assert.Equal(t, "5", string(series[2].Name()))
}
