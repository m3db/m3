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
	"io/ioutil"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// GeneratePromWriteRequest generates a Prometheus remote
// write request.
func GeneratePromWriteRequest() *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: []byte(model.MetricNameLabel), Value: []byte("first")},
					{Name: []byte("foo"), Value: []byte("bar")},
					{Name: []byte("biz"), Value: []byte("baz")},
				},
				Samples: []prompb.Sample{
					{Value: 1.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 2.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: []byte(model.MetricNameLabel), Value: []byte("second")},
					{Name: []byte("foo"), Value: []byte("qux")},
					{Name: []byte("bar"), Value: []byte("baz")},
				},
				Samples: []prompb.Sample{
					{Value: 3.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 4.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			},
		},
	}
	return req
}

// GeneratePromWriteRequestBody generates a Prometheus remote
// write request body.
func GeneratePromWriteRequestBody(
	t require.TestingT,
	req *prompb.WriteRequest,
) io.Reader {
	return bytes.NewReader(GeneratePromWriteRequestBodyBytes(t, req))
}

// GeneratePromWriteRequestBodyBytes generates a Prometheus remote
// write request body.
func GeneratePromWriteRequestBodyBytes(
	t require.TestingT,
	req *prompb.WriteRequest,
) []byte {
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	return compressed
}

// ReadPromWriteRequestBody reads a Prometheus remote
// write request body.
func ReadPromWriteRequestBody(
	t require.TestingT,
	body io.Reader,
) *prompb.WriteRequest {
	require.NotNil(t, body)

	data, err := ioutil.ReadAll(body)
	require.NoError(t, err)

	decoded, err := snappy.Decode(nil, data)
	require.NoError(t, err)

	req := &prompb.WriteRequest{}
	require.NoError(t, proto.Unmarshal(decoded, req))

	return req
}
