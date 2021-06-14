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

package annotated

import (
	"testing"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimestampMillis = 1575000000000

var testTimeseries = []prompb.AnnotatedTimeSeries{
	{
		Labels: []prompb.Label{
			{Name: []byte("__name__"), Value: []byte("requests")},
			{Name: []byte("status_code"), Value: []byte("200")},
		},
		Samples: []prompb.AnnotatedSample{
			{
				Value:      4.2,
				Timestamp:  testTimestampMillis,
				Annotation: []byte("foo"),
			},
			{
				Value:      3.14,
				Timestamp:  testTimestampMillis + 10000,
				Annotation: []byte("bar"),
			},
		},
	},
	{
		Labels: []prompb.Label{
			{Name: []byte("__name__"), Value: []byte("requests")},
			{Name: []byte("status_code"), Value: []byte("500")},
		},
		Samples: []prompb.AnnotatedSample{
			{
				Value:      6.28,
				Timestamp:  testTimestampMillis,
				Annotation: []byte("baz"),
			},
		},
	},
}

func TestIter(t *testing.T) {
	tests := []struct {
		name  string
		input []prompb.AnnotatedTimeSeries
		wants []iterOutput
	}{
		{
			name:  "valid input",
			input: testTimeseries,
			wants: []iterOutput{
				{
					tags: models.Tags{
						Opts: models.NewTagOptions(),
						Tags: []models.Tag{
							{Name: []byte("__name__"), Value: []byte("requests")},
							{Name: []byte("status_code"), Value: []byte("200")},
						},
					},
					datapoints: ts.Datapoints{
						ts.Datapoint{
							Timestamp: xtime.ToUnixNano(storage.PromTimestampToTime(testTimestampMillis)),
							Value:     4.2,
						},
					},
					unit:       xtime.Millisecond,
					annotation: []byte("foo"),
				},
				{
					tags: models.Tags{
						Opts: models.NewTagOptions(),
						Tags: []models.Tag{
							{Name: []byte("__name__"), Value: []byte("requests")},
							{Name: []byte("status_code"), Value: []byte("200")},
						},
					},
					datapoints: ts.Datapoints{
						ts.Datapoint{
							Timestamp: xtime.ToUnixNano(storage.PromTimestampToTime(testTimestampMillis + 10000)),
							Value:     3.14,
						},
					},
					unit:       xtime.Millisecond,
					annotation: []byte("bar"),
				},
				{
					tags: models.Tags{
						Opts: models.NewTagOptions(),
						Tags: []models.Tag{
							{Name: []byte("__name__"), Value: []byte("requests")},
							{Name: []byte("status_code"), Value: []byte("500")},
						},
					},
					datapoints: ts.Datapoints{
						ts.Datapoint{
							Timestamp: xtime.ToUnixNano(storage.PromTimestampToTime(testTimestampMillis)),
							Value:     6.28,
						},
					},
					unit:       xtime.Millisecond,
					annotation: []byte("baz"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := newIter(tt.input, models.NewTagOptions())
			for _, want := range tt.wants {
				testOutput(t, iter, want)
			}

			// Range over the wants values a second time so we can test resetting the iterator.
			require.Nil(t, iter.Reset())
			for _, e := range tt.wants {
				testOutput(t, iter, e)
			}

			require.Nil(t, iter.Error())
		})
	}
}

func testOutput(t *testing.T, iter *iter, want iterOutput) {
	require.True(t, iter.Next())

	value := iter.Current()
	assert.True(t, want.tags.Equals(value.Tags))
	assert.Equal(t, want.datapoints, value.Datapoints)
	assert.Equal(t, want.unit, value.Unit)
	assert.Equal(t, want.annotation, value.Annotation)
}

type iterOutput struct {
	tags       models.Tags
	datapoints ts.Datapoints
	unit       xtime.Unit
	annotation []byte
}
