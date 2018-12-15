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
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"
	xtest "github.com/m3db/m3/src/x/test"

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
	vals.Add(stepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func TestParamParsing(t *testing.T) {
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	r, err := parseParams(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInstantaneousParamParsing(t *testing.T) {
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	params := url.Values{}
	now := time.Now()
	params.Add(queryParam, promQuery)
	params.Add(timeParam, now.Format(time.RFC3339))
	req.URL.RawQuery = params.Encode()

	r, err := parseInstantaneousParams(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r.Query)
}

func TestInvalidStart(t *testing.T) {
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(startParam)
	req.URL.RawQuery = vals.Encode()
	_, err := parseParams(req)
	require.NotNil(t, err, "unable to parse request")
	require.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestInvalidTarget(t *testing.T) {
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	vals := defaultParams()
	vals.Del(queryParam)
	req.URL.RawQuery = vals.Encode()

	p, err := parseParams(req)
	require.NotNil(t, err, "unable to parse request")
	assert.NotNil(t, p.Start)
	require.Equal(t, err.Code(), http.StatusBadRequest)
}

func TestParseDuration(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=10s", nil)
	require.NoError(t, err)
	v, err := parseDuration(r, stepParam)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, v)
}

func TestParseDurationParsesIntAsSeconds(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=30", nil)
	require.NoError(t, err)
	v, err := parseDuration(r, stepParam)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, v)
}

func TestParseDurationError(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=bar10", nil)
	require.NoError(t, err)
	_, err = parseDuration(r, stepParam)
	assert.Error(t, err)
}

func TestRenderResultsJSON(t *testing.T) {
	start := time.Unix(1535948880, 0)
	buffer := bytes.NewBuffer(nil)
	params := models.RequestParams{}
	series := []*ts.Series{
		ts.NewSeries("foo", ts.NewFixedStepValues(10*time.Second, 2, 1, start), test.TagSliceToTags([]models.Tag{
			models.Tag{Name: []byte("bar"), Value: []byte("baz")},
			models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
		})),
		ts.NewSeries("bar", ts.NewFixedStepValues(10*time.Second, 2, 2, start), test.TagSliceToTags([]models.Tag{
			models.Tag{Name: []byte("baz"), Value: []byte("bar")},
			models.Tag{Name: []byte("qaz"), Value: []byte("qux")},
		})),
	}

	renderResultsJSON(buffer, series, params)

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
	// params := models.RequestParams{}
	series := []*ts.Series{
		ts.NewSeries("foo", ts.NewFixedStepValues(10*time.Second, 1, 1, start), test.TagSliceToTags([]models.Tag{
			models.Tag{Name: []byte("bar"), Value: []byte("baz")},
			models.Tag{Name: []byte("qux"), Value: []byte("qaz")},
		})),
		ts.NewSeries("bar", ts.NewFixedStepValues(10*time.Second, 1, 2, start), test.TagSliceToTags([]models.Tag{
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
