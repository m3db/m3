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
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	promQuery = `http_requests_total{job="prometheus",group="canary"}`
)

func defaultParams() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(targetParam, promQuery)
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
	require.Equal(t, promQuery, r.Target)
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
	vals.Del(targetParam)
	req.URL.RawQuery = vals.Encode()

	p, err := parseParams(req)
	require.NotNil(t, err, "unable to parse request")
	assert.NotNil(t, p.Start)
	require.Equal(t, err.Code(), http.StatusBadRequest)
}
