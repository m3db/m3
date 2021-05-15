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

package test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

// SlowHandler slows down a request by delay
type SlowHandler struct {
	handler http.Handler
	delay   time.Duration
}

// NewSlowHandler creates a new slow handler
func NewSlowHandler(handler http.Handler, delay time.Duration) *SlowHandler {
	return &SlowHandler{handler: handler, delay: delay}
}

// ServeHTTP implements http.handler
func (h *SlowHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(h.delay)
	h.handler.ServeHTTP(w, r)
}

// GeneratePromReadRequest generates a sample prometheus remote read request
func GeneratePromReadRequest() *prompb.ReadRequest {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: []byte("eq"), Value: []byte("a")},
			},
			StartTimestampMs: time.Now().Add(-1*time.Hour*24).UnixNano() / int64(time.Millisecond),
			EndTimestampMs:   time.Now().UnixNano() / int64(time.Millisecond),
		}},
	}
	return req
}

// GeneratePromReadBody generates a sample snappy encoded prometheus remote read request body
func GeneratePromReadBody(t *testing.T) io.Reader {
	req := GeneratePromReadRequest()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}

	compressed := snappy.Encode(nil, data)
	// Uncomment the line below to write the data into a file useful for integration testing
	//ioutil.WriteFile("/tmp/dat1", compressed, 0644)
	b := bytes.NewReader(compressed)
	return b
}
