// Copyright (c) 2019 Uber Technologies, Inc.
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

package remote

import (
	"bytes"
	"container/heap"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/query/generated/proto/prompb"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestParseWriteRequest(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	nowMillis := now.UnixNano() / int64(time.Millisecond)

	input := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels: []*prompb.Label{
					&prompb.Label{Name: b("foo"), Value: b("bar")},
					&prompb.Label{Name: b("bar"), Value: b("baz")},
				},
				Samples: []*prompb.Sample{
					&prompb.Sample{
						Value:     42.0,
						Timestamp: nowMillis,
					},
					&prompb.Sample{
						Value:     84.0,
						Timestamp: nowMillis,
					},
				},
			},
			&prompb.TimeSeries{
				Labels: []*prompb.Label{
					&prompb.Label{Name: b("qux"), Value: b("qar")},
					&prompb.Label{Name: b("qar"), Value: b("qaz")},
				},
				Samples: []*prompb.Sample{
					&prompb.Sample{
						Value:     123.0,
						Timestamp: nowMillis,
					},
				},
			},
		},
	}

	data, err := proto.Marshal(input)
	require.NoError(t, err)

	encoded := snappy.Encode(nil, data)

	httpReq := httptest.NewRequest("POST", "/write", bytes.NewReader(encoded))
	httpReq.ContentLength = int64(len(encoded))

	_, err = NewParser().ParseWriteRequest(httpReq)
	require.NoError(t, err)

	// Ensure same as input except with labels sorted.
	// expected, err := json.Marshal(&WriteRequest{
	// 	Series: []WriteSeries{
	// 		WriteSeries{
	// 			Labels: []Label{
	// 				Label{Name: b("bar"), Value: b("baz")},
	// 				Label{Name: b("foo"), Value: b("bar")},
	// 			},
	// 			Samples: []Sample{
	// 				Sample{
	// 					Value:          42.0,
	// 					TimeUnixMillis: nowMillis,
	// 				},
	// 				Sample{
	// 					Value:          84.0,
	// 					TimeUnixMillis: nowMillis,
	// 				},
	// 			},
	// 		},
	// 		WriteSeries{
	// 			Labels: []Label{
	// 				Label{Name: b("qar"), Value: b("qaz")},
	// 				Label{Name: b("qux"), Value: b("qar")},
	// 			},
	// 			Samples: []Sample{
	// 				Sample{
	// 					Value:          123.0,
	// 					TimeUnixMillis: nowMillis,
	// 				},
	// 			},
	// 		},
	// 	},
	// })
	// require.NoError(t, err)

	// actual, err := json.Marshal(req)
	// require.NoError(t, err)

	// require.Equal(t, expected, actual)
}

func TestBytesHeap(t *testing.T) {
	h := &bytesHeap{}
	heap.Init(h)

	heap.Push(h, make([]byte, 0, 3))
	heap.Push(h, make([]byte, 0, 5))
	heap.Push(h, make([]byte, 0, 4))

	v := heap.Pop(h)
	assert.Equal(t, 5, cap(v.([]byte)))
}
