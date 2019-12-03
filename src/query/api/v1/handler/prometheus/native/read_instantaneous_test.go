// Copyright (c) 2019 Uber Technologies, Inc.
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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vectorResult struct {
	Data struct {
		Result []struct {
			Metric map[string]string  `json:"metric"`
			Value  vectorResultValues `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

type vectorResultValues []interface{}

func (v vectorResultValues) parse() (time.Time, int, error) {
	if len(v) != 2 {
		return time.Time{}, 0,
			fmt.Errorf("expected length 2: actual=%d", len(v))
	}

	t, ok := v[0].(float64)
	if !ok {
		return time.Time{}, 0,
			fmt.Errorf("could not unmarshal time: %v", v[0])
	}

	str, ok := v[1].(string)
	if !ok {
		return time.Time{}, 0,
			fmt.Errorf("could not unmarshal value: %v", v[1])
	}

	n, err := strconv.Atoi(str)
	if err != nil {
		return time.Time{}, 0,
			fmt.Errorf("could not convert value to number: err=%v", err)
	}

	return time.Unix(int64(t), 0), n, nil
}

func TestPromReadInstantHandler(t *testing.T) {
	testPromReadInstantHandler(t, block.NewResultMetadata(), "")
	testPromReadInstantHandler(t, buildWarningMeta("foo", "bar"), "foo_bar")
	testPromReadInstantHandler(t, block.ResultMetadata{Exhaustive: false},
		handler.LimitHeaderSeriesLimitApplied)
}

func testPromReadInstantHandler(
	t *testing.T,
	resultMeta block.ResultMetadata,
	ex string,
) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)

	setup := newTestSetup()
	promReadInstant := setup.Handlers.InstantRead

	seriesMeta := test.NewSeriesMeta("dummy", len(values))
	meta := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(0, models.NewTagOptions()),
		ResultMetadata: resultMeta,
	}

	b := test.NewBlockFromValuesWithMetaAndSeriesMeta(meta, seriesMeta, values)
	test.NewBlockFromValues(bounds, values)
	setup.Storage.SetFetchBlocksResult(block.Result{Blocks: []block.Block{b}}, nil)

	req := httptest.NewRequest(PromReadInstantHTTPMethods[0], PromReadInstantURL, nil)

	params := url.Values{}
	params.Set(queryParam, "dummy0{}")

	req.URL.RawQuery = params.Encode()

	recorder := httptest.NewRecorder()
	promReadInstant.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

	header := recorder.Header().Get(handler.LimitHeader)
	assert.Equal(t, ex, header)

	var result vectorResult
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Equal(t, 2, len(result.Data.Result))

	at0, value0, err := result.Data.Result[0].Value.parse()
	require.NoError(t, err)
	at1, value1, err := result.Data.Result[1].Value.parse()
	require.NoError(t, err)

	expected := mustPrettyJSON(t, fmt.Sprintf(`
	{
		"status": "success",
		"data": {
			"resultType": "vector",
			"result": [
				{
					"metric": {
						"__name__": "dummy0",
						"dummy0": "dummy0"
					},
					"value": [
						%d,
						"%d"
					]
				},
				{
					"metric": {
						"__name__": "dummy1",
						"dummy1": "dummy1"
					},
					"value": [
						%d,
						"%d"
					]
				}
			]
		}
	}
	`, at0.Unix(), value0, at1.Unix(), value1))
	actual := mustPrettyJSON(t, recorder.Body.String())
	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestPromReadInstantHandlerStorageError(t *testing.T) {
	setup := newTestSetup()
	promReadInstant := setup.Handlers.InstantRead

	storageErr := fmt.Errorf("storage err")
	setup.Storage.SetFetchBlocksResult(block.Result{}, storageErr)

	req := httptest.NewRequest(PromReadInstantHTTPMethods[0], PromReadInstantURL, nil)

	params := url.Values{}
	params.Set(queryParam, "dummy0{}")

	req.URL.RawQuery = params.Encode()

	recorder := httptest.NewRecorder()
	promReadInstant.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusInternalServerError, recorder.Result().StatusCode)

	var errResp struct {
		Error string `json:"error"`
	}
	resp := recorder.Body.Bytes()
	require.NoError(t, json.Unmarshal(resp, &errResp))
	require.Equal(t, storageErr.Error(), errResp.Error)
}
