// Copyright (c) 2021 Uber Technologies, Inc.
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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
)

func TestQueryResponse(t *testing.T) {
	startTime := time.Now().Round(0)
	endTime := startTime.Add(time.Hour)
	cases := []struct {
		name      string
		code      int
		threshold time.Duration
		form      map[string]string
		headers   map[string]string
		fields    map[string]interface{}
	}{
		{
			name:      "happy path",
			code:      200,
			threshold: time.Microsecond,
			form: map[string]string{
				"query": "fooquery",
				"start": startTime.Format(time.RFC3339Nano),
				"end":   endTime.Format(time.RFC3339Nano),
				"extra": "foobar",
			},
			headers: map[string]string{
				headers.LimitHeader:        "10",
				headers.LimitMaxDocsHeader: "100",
				"foo":                      "bar",
			},
			fields: map[string]interface{}{
				"query":                    "fooquery",
				"start":                    startTime,
				"end":                      endTime,
				"status":                   int64(200),
				"queryRange":               time.Hour,
				headers.LimitHeader:        "10",
				headers.LimitMaxDocsHeader: "100",
			},
		},
		{
			name:      "no request data",
			code:      504,
			threshold: time.Microsecond,
			fields: map[string]interface{}{
				"query":      "",
				"start":      time.Time{},
				"end":        time.Time{},
				"status":     int64(504),
				"queryRange": time.Duration(0),
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.InfoLevel)
			iOpts := instrument.NewOptions().SetLogger(zap.New(core))
			h := QueryResponse(tc.threshold, iOpts).Middleware(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(tc.threshold * 2)
					w.WriteHeader(tc.code)
				}))
			r := mux.NewRouter()
			r.Handle("/testRoute", h)
			server := httptest.NewServer(r)
			defer server.Close()

			values := url.Values{}
			for k, v := range tc.form {
				values.Add(k, v)
			}
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/testRoute",
				strings.NewReader(values.Encode()))
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, err)
			resp, err := server.Client().Do(req)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, err)
			require.Equal(t, tc.code, resp.StatusCode)
			msgs := recorded.FilterMessage("finished handling query request").All()
			require.Len(t, msgs, 1)
			fields := msgs[0].ContextMap()
			require.Equal(t, "/testRoute", fields["url"])
			require.True(t, fields["duration"].(time.Duration) >= tc.threshold*2)

			for k, v := range tc.fields {
				require.Equal(t, v, fields[k], "log field %v", k)
			}
			require.Len(t, fields, len(tc.fields)+2)
		})
	}
}
