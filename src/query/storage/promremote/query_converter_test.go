// Copyright (c) 2021  Uber Technologies, Inc.
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

package promremote

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestQueryConverter(t *testing.T) {
	now := xtime.Now()
	tcs := []struct {
		name     string
		input    storage.WriteQueryOptions
		expected *prompb.WriteRequest
	}{
		{
			name: "converts",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions(),
					Tags: []models.Tag{{
						Name:  []byte("test_tag_name"),
						Value: []byte("test_tag_value"),
					}},
				},
				Datapoints: ts.Datapoints{{
					Timestamp: now,
					Value:     42,
				}},
				Unit: xtime.Millisecond,
			},
			expected: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{
								Name:  "test_tag_name",
								Value: "test_tag_value",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: now.ToNormalizedTime(time.Millisecond),
								Value:     42,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			q, err := storage.NewWriteQuery(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, convertAndEncodeWriteQuery(q))
		})
	}
}

func TestConvertQueryNil(t *testing.T) {
	assert.Nil(t, convertAndEncodeWriteQuery(nil))
}

func TestEncodeWriteQuery(t *testing.T) {
	data, err := encodeWriteQuery(nil)
	require.Error(t, err)
	assert.Len(t, data, 0)
	assert.Contains(t, err.Error(), "received nil query")
}
