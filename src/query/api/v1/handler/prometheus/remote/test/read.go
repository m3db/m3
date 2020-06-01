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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// GeneratePromReadRequest generates a Prometheus remote
// read request.
func GeneratePromReadRequest() *prompb.ReadRequest {
	return &prompb.ReadRequest{
		Queries: []*prompb.Query{
			&prompb.Query{
				StartTimestampMs: time.Now().UnixNano()/int64(time.Millisecond) - int64(60000*time.Millisecond),
				EndTimestampMs:   time.Now().UnixNano() / int64(time.Millisecond),
				Matchers: []*prompb.LabelMatcher{
					&prompb.LabelMatcher{
						Name:  []byte(model.MetricNameLabel),
						Value: []byte("first"),
						Type:  prompb.LabelMatcher_EQ,
					},
				},
			},
		},
	}
}

// GeneratePromReadRequestBody generates a Prometheus remote
// read request body.
func GeneratePromReadRequestBody(
	t *testing.T,
	req *prompb.ReadRequest,
) io.Reader {
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	return bytes.NewReader(compressed)
}
