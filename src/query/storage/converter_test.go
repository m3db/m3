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

package storage

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelConversion(t *testing.T) {
	// NB: sorted order (__name__, foo, le)
	labels := []prompb.Label{
		prompb.Label{Name: promDefaultName, Value: []byte("name-val")},
		prompb.Label{Name: []byte("foo"), Value: []byte("bar")},
		prompb.Label{Name: promDefaultBucketName, Value: []byte("bucket-val")},
	}

	opts := models.NewTagOptions().
		SetMetricName([]byte("name")).
		SetBucketName([]byte("bucket"))

	tags := PromLabelsToM3Tags(labels, opts)
	name, found := tags.Name()
	assert.True(t, found)
	assert.Equal(t, []byte("name-val"), name)
	name, found = tags.Get([]byte("name"))
	assert.True(t, found)
	assert.Equal(t, []byte("name-val"), name)

	bucket, found := tags.Bucket()
	assert.True(t, found)
	assert.Equal(t, []byte("bucket-val"), bucket)
	bucket, found = tags.Get([]byte("bucket"))
	assert.True(t, found)
	assert.Equal(t, []byte("bucket-val"), bucket)

	reverted := TagsToPromLabels(tags)
	assert.Equal(t, labels, reverted)
}

var (
	name  = []byte("foo")
	value = []byte("bar")
)

func TestPromReadQueryToM3(t *testing.T) {
	tests := []struct {
		name        string
		matchers    []*prompb.LabelMatcher
		expected    []*models.Matcher
		expectError bool
	}{
		{
			name: "single exact match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: name, Value: value},
			},
		},
		{
			name: "single exact match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotEqual, Name: name, Value: value},
			},
		},
		{
			name: "single regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchRegexp, Name: name, Value: value},
			},
		},
		{
			name: "single regexp match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NRE, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotRegexp, Name: name, Value: value},
			},
		},
		{
			name: "mixed exact match and regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: name, Value: value},
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: []byte("baz"), Value: []byte("qux")},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: name, Value: value},
				&models.Matcher{Type: models.MatchRegexp, Name: []byte("baz"), Value: []byte("qux")},
			},
		},
		{
			name: "unrecognized matcher type",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_Type(math.MaxInt32), Name: name, Value: value},
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := &prompb.Query{
				StartTimestampMs: 123000,
				EndTimestampMs:   456000,
				Matchers:         test.matchers,
			}
			result, err := PromReadQueryToM3(input)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.True(t, result.Start.Equal(time.Unix(123, 0)), "start does not match")
				assert.True(t, result.End.Equal(time.Unix(456, 0)), "end does not match")
				require.Equal(t, len(test.expected), len(result.TagMatchers),
					"tag matchers length not match")
				for i, expected := range test.expected {
					expectedStr := expected.String()
					actualStr := result.TagMatchers[i].String()
					assert.Equal(t, expectedStr, actualStr,
						fmt.Sprintf("matcher does not match: idx=%d, expected=%s, actual=%s",
							i, expectedStr, actualStr))
				}
			}
		})
	}
}

var (
	benchResult *prompb.QueryResult
)

func TestFetchResultToPromResult(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := TimeToPromTimestamp(now)

	vals := ts.NewMockValues(ctrl)
	vals.EXPECT().Len().Return(0).Times(2)
	vals.EXPECT().Datapoints().Return(ts.Datapoints{})

	tags := models.NewTags(1, models.NewTagOptions()).
		AddTag(models.Tag{Name: []byte("a"), Value: []byte("b")})

	valsNonEmpty := ts.NewMockValues(ctrl)
	valsNonEmpty.EXPECT().Len().Return(1).Times(3)
	dp := ts.Datapoints{{Timestamp: now, Value: 1}}
	valsNonEmpty.EXPECT().Datapoints().Return(dp).Times(2)
	tagsNonEmpty := models.NewTags(1, models.NewTagOptions()).
		AddTag(models.Tag{Name: []byte("c"), Value: []byte("d")})

	r := &FetchResult{
		SeriesList: ts.SeriesList{
			ts.NewSeries([]byte("a"), vals, tags),
			ts.NewSeries([]byte("c"), valsNonEmpty, tagsNonEmpty),
		},
	}

	// NB: not keeping empty series.
	result := FetchResultToPromResult(r, false)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	assert.Equal(t, expected, result)

	// NB: keeping empty series.
	result = FetchResultToPromResult(r, true)
	expected = &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{},
			},
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	assert.Equal(t, expected, result)
}

// BenchmarkFetchResultToPromResult-8   	     100	  10563444 ns/op	25368543 B/op	    4443 allocs/op
func BenchmarkFetchResultToPromResult(b *testing.B) {
	var (
		numSeries              = 1000
		numDatapointsPerSeries = 1000
		numTagsPerSeries       = 10
		fr                     = &FetchResult{
			SeriesList: make(ts.SeriesList, 0, numSeries),
		}
	)

	for i := 0; i < numSeries; i++ {
		values := make(ts.Datapoints, 0, numDatapointsPerSeries)
		for i := 0; i < numDatapointsPerSeries; i++ {
			values = append(values, ts.Datapoint{
				Timestamp: time.Time{},
				Value:     float64(i),
			})
		}

		tags := models.NewTags(numTagsPerSeries, nil)
		for i := 0; i < numTagsPerSeries; i++ {
			tags = tags.AddTag(models.Tag{
				Name:  []byte(fmt.Sprintf("name-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
		}

		series := ts.NewSeries(
			[]byte(fmt.Sprintf("series-%d", i)), values, tags)

		fr.SeriesList = append(fr.SeriesList, series)
	}

	for i := 0; i < b.N; i++ {
		benchResult = FetchResultToPromResult(fr, false)
	}
}
