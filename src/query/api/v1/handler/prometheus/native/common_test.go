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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"
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
