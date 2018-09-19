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
	"github.com/stretchr/testify/require"
)

// GeneratePromWriteRequest generates a Prometheus remote
// write request.
func GeneratePromWriteRequest() *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "first"},
				{Name: "foo", Value: "bar"},
				{Name: "biz", Value: "baz"},
			},
			Samples: []*prompb.Sample{
				{Value: 1.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				{Value: 2.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
			},
		},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "second"},
					{Name: "foo", Value: "qux"},
					{Name: "bar", Value: "baz"},
				},
				Samples: []*prompb.Sample{
					{Value: 3.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 4.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			}},
	}
	return req
}

// GeneratePromWriteRequestBody generates a Prometheus remote
// write request body.
func GeneratePromWriteRequestBody(
	t *testing.T,
	req *prompb.WriteRequest,
) io.Reader {
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	return bytes.NewReader(compressed)
}
