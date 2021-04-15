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

package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/test"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromCompressedReadSuccess(t *testing.T) {
	req := httptest.NewRequest("POST", "/dummy", test.GeneratePromReadBody(t))
	_, err := ParsePromCompressedRequest(req)
	assert.NoError(t, err)
}

func TestPromCompressedReadNoBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/dummy", nil)
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestPromCompressedReadEmptyBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/dummy", bytes.NewReader([]byte{}))
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestPromCompressedReadInvalidEncoding(t *testing.T) {
	req := httptest.NewRequest("POST", "/dummy", bytes.NewReader([]byte{'a'}))
	_, err := ParsePromCompressedRequest(req)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

type writer struct {
	value string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.value = string(p)
	return len(p), nil
}

type tag struct {
	name, value string
}

func toTags(name string, tags ...tag) models.Metric {
	tagOpts := models.NewTagOptions()
	ts := models.NewTags(len(tags), tagOpts)
	ts = ts.SetName([]byte(name))
	for _, tag := range tags {
		ts = ts.AddTag(models.Tag{Name: []byte(tag.name), Value: []byte(tag.value)})
	}

	return models.Metric{Tags: ts}
}

func TestParseStartAndEnd(t *testing.T) {
	endTime := time.Now().Truncate(time.Hour)
	opts := promql.NewParseOptions().SetNowFn(func() time.Time { return endTime })

	tests := []struct {
		querystring string
		exStart     time.Time
		exEnd       time.Time
		exErr       bool
	}{
		{querystring: "", exStart: time.Unix(0, 0), exEnd: endTime},
		{querystring: "start=100", exStart: time.Unix(100, 0), exEnd: endTime},
		{querystring: "start=100&end=200", exStart: time.Unix(100, 0), exEnd: time.Unix(200, 0)},
		{querystring: "start=200&end=100", exErr: true},
		{querystring: "start=foo&end=100", exErr: true},
		{querystring: "start=100&end=bar", exErr: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("GET_%s", tt.querystring), func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
				fmt.Sprintf("/?%s", tt.querystring), nil)
			require.NoError(t, err)

			start, end, err := ParseStartAndEnd(req, opts)
			if tt.exErr {
				require.Error(t, err)
			} else {
				assert.Equal(t, tt.exStart, start)
				assert.Equal(t, tt.exEnd, end)
			}
		})
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("POST_%s", tt.querystring), func(t *testing.T) {
			b := bytes.NewBuffer([]byte(tt.querystring))
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", b)
			require.NoError(t, err)
			req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeFormURLEncoded)

			start, end, err := ParseStartAndEnd(req, opts)
			if tt.exErr {
				require.Error(t, err)
			} else {
				assert.Equal(t, tt.exStart, start)
				assert.Equal(t, tt.exEnd, end)
			}
		})
	}
}

func TestParseRequireStartEnd(t *testing.T) {
	opts := promql.NewParseOptions()

	tests := []struct {
		exStart         time.Time
		querystring     string
		requireStartEnd bool
		exErr           bool
	}{
		{querystring: "start=100", requireStartEnd: true, exStart: time.Unix(100, 0)},
		{querystring: "", requireStartEnd: true, exErr: true},
		{querystring: "start=100", requireStartEnd: false, exStart: time.Unix(100, 0)},
		{querystring: "", requireStartEnd: false, exStart: time.Unix(0, 0)},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("GET_%s", tt.querystring), func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
				fmt.Sprintf("/?%s", tt.querystring), nil)
			require.NoError(t, err)

			start, _, err := ParseStartAndEnd(req, opts.SetRequireStartEndTime(tt.requireStartEnd))
			if tt.exErr {
				require.Error(t, err)
			} else {
				assert.Equal(t, tt.exStart, start)
			}
		})
	}
}

// TestParseMatch tests the parsing / construction logic around ParseMatch().
// matcher_test.go has more comprehensive testing on parsing details.
func TestParseMatch(t *testing.T) {
	parseOpts := promql.NewParseOptions()
	tagOpts := models.NewTagOptions()

	tests := []struct {
		querystring string
		exMatch     []ParsedMatch
		exErr       bool
		exEmpty     bool
	}{
		{exEmpty: true},
		{
			querystring: "match[]=eq_label",
			exMatch: []ParsedMatch{
				{
					Match: "eq_label",
					Matchers: models.Matchers{
						{
							Type:  models.MatchEqual,
							Name:  []byte("__name__"),
							Value: []byte("eq_label"),
						},
					},
				},
			},
		},
		{querystring: "match[]=illegal%match", exErr: true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("GET_%s", tt.querystring), func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
				fmt.Sprintf("/?%s", tt.querystring), nil)
			require.NoError(t, err)

			parsedMatches, ok, err := ParseMatch(req, parseOpts, tagOpts)

			if tt.exErr {
				require.Error(t, err)
				require.False(t, ok)
				require.Empty(t, parsedMatches)
				return
			}

			require.NoError(t, err)
			if tt.exEmpty {
				require.False(t, ok)
				require.Empty(t, parsedMatches)
			} else {
				require.True(t, ok)
				require.Equal(t, tt.exMatch, parsedMatches)
			}
		})
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("POST_%s", tt.querystring), func(t *testing.T) {
			b := bytes.NewBuffer([]byte(tt.querystring))
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/", b)
			require.NoError(t, err)
			req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeFormURLEncoded)

			parsedMatches, ok, err := ParseMatch(req, parseOpts, tagOpts)

			if tt.exErr {
				require.Error(t, err)
				require.False(t, ok)
				require.Empty(t, parsedMatches)
				return
			}

			require.NoError(t, err)
			if tt.exEmpty {
				require.False(t, ok)
				require.Empty(t, parsedMatches)
			} else {
				require.True(t, ok)
				require.Equal(t, tt.exMatch, parsedMatches)
			}
		})
	}
}
