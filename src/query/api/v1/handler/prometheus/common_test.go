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
	"strings"
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

func TestRenderSeriesMatchResultsNoTags(t *testing.T) {
	w := &writer{value: ""}
	tests := []struct {
		dropRole   bool
		additional string
	}{
		{
			dropRole:   true,
			additional: "",
		},
		{
			dropRole:   false,
			additional: `,"role":"appears"`,
		},
	}

	seriesMatchResult := []models.Metrics{
		{
			toTags("name", tag{name: "a", value: "b"}, tag{name: "role", value: "appears"}),
			toTags("name2", tag{name: "c", value: "d"}, tag{name: "e", value: "f"}),
		},
	}

	for _, tt := range tests {
		expectedWhitespace := fmt.Sprintf(`{
		"status":"success",
		"data":[
			{
				"__name__":"name",
				"a":"b"%s
			},
			{
				"__name__":"name2",
				"c":"d",
				"e":"f"
			}
		]
	}`, tt.additional)

		err := RenderSeriesMatchResultsJSON(w, seriesMatchResult, tt.dropRole)
		assert.NoError(t, err)
		fields := strings.Fields(expectedWhitespace)
		expected := ""
		for _, field := range fields {
			expected = expected + field
		}

		assert.Equal(t, expected, w.value)
	}
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
