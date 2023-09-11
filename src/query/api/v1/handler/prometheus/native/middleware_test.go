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
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestQueryResponse(t *testing.T) {
	now := time.Now().UTC().Round(0)
	startTime := now
	endTime := startTime.Add(time.Hour)
	cases := []struct {
		name            string
		code            int
		duration        time.Duration
		threshold       time.Duration
		disabled        bool
		err             error
		form            map[string]string
		requestHeaders  map[string]string
		responseHeaders map[string]string
		fields          map[string]interface{}
	}{
		{
			name:      "happy path",
			code:      200,
			duration:  time.Second,
			threshold: time.Microsecond,
			form: map[string]string{
				"query": "fooquery",
				"start": startTime.Format(time.RFC3339Nano),
				"end":   endTime.Format(time.RFC3339Nano),
				"extra": "foobar",
			},
			requestHeaders: map[string]string{
				headers.LimitHeader:        "10",
				headers.LimitMaxDocsHeader: "100",
				"foo":                      "bar",
			},
			responseHeaders: map[string]string{
				headers.TimeoutHeader:                 "10",
				headers.ReturnedMetadataLimitedHeader: "100",
				"foo":                                 "bar",
			},
			fields: map[string]interface{}{
				"query":                               "fooquery",
				"start":                               startTime,
				"end":                                 endTime,
				"status":                              int64(200),
				"queryRange":                          time.Hour,
				headers.LimitHeader:                   "10",
				headers.LimitMaxDocsHeader:            "100",
				headers.TimeoutHeader:                 "10",
				headers.ReturnedMetadataLimitedHeader: "100",
			},
		},
		{
			name:      "no request data",
			code:      504,
			duration:  time.Second,
			threshold: time.Microsecond,
			fields: map[string]interface{}{
				"query":      "",
				"start":      time.Time{},
				"end":        time.Time{},
				"status":     int64(504),
				"queryRange": time.Duration(0),
			},
		},
		{
			name: "instant",
			form: map[string]string{
				"query": "fooquery",
				"start": "now",
				"end":   "now",
			},
			code:      200,
			duration:  time.Second,
			threshold: time.Microsecond,
			fields: map[string]interface{}{
				"query":      "fooquery",
				"start":      now,
				"end":        now,
				"status":     int64(200),
				"queryRange": time.Duration(0),
			},
		},
		{
			name: "error",
			form: map[string]string{
				"query": "fooquery",
				"start": "now",
				"end":   "now",
			},
			code:      500,
			err:       errors.New("boom"),
			duration:  time.Second,
			threshold: time.Microsecond,
			fields: map[string]interface{}{
				"query":      "fooquery",
				"start":      now,
				"end":        now,
				"status":     int64(500),
				"queryRange": time.Duration(0),
				"error":      "boom",
			},
		},
		{
			name: "below threshold",
			form: map[string]string{
				"query": "fooquery",
				"start": "now",
				"end":   "now",
			},
			code:      200,
			duration:  time.Millisecond,
			threshold: time.Second,
		},
		{
			name: "disabled",
			form: map[string]string{
				"query": "fooquery",
				"start": "now",
				"end":   "now",
			},
			code:      200,
			disabled:  true,
			duration:  time.Second,
			threshold: time.Millisecond,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.InfoLevel)
			iOpts := instrument.NewOptions().SetLogger(zap.New(core))
			clock := clockwork.NewFakeClockAt(now)
			opts := WithQueryParams(middleware.Options{
				InstrumentOpts: iOpts,
				Clock:          clock,
				Metrics:        middleware.MetricsOptions{},
				Logging: middleware.LoggingOptions{
					Threshold: tc.threshold,
					Disabled:  tc.disabled,
				},
			})
			h := middleware.ResponseLogging(opts).Middleware(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					clock.Advance(tc.duration)
					if tc.err != nil {
						xhttp.WriteError(w, tc.err)
						return
					}
					for k, v := range tc.responseHeaders {
						w.Header().Set(k, v)
					}
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
			req, err := http.NewRequestWithContext(context.Background(), "POST", server.URL+"/testRoute",
				strings.NewReader(values.Encode()))
			for k, v := range tc.requestHeaders {
				req.Header.Set(k, v)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, err)
			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, tc.code, resp.StatusCode)
			msgs := recorded.FilterMessage("finished handling request").All()
			if len(tc.fields) == 0 {
				require.Len(t, msgs, 0)
			} else {
				require.Len(t, msgs, 1)
				fields := msgs[0].ContextMap()
				require.Equal(t, "/testRoute", fields["url"])
				require.True(t, fields["duration"].(time.Duration) >= tc.duration)

				for k, v := range tc.fields {
					require.Equal(t, v, fields[k], "log field %v", k)
				}
				require.Len(t, fields, len(tc.fields)+2)
			}
		})
	}
}
