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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testID   = ident.StringID("test_id")
	testTags = []models.Tag{
		{Name: []byte("t1"), Value: []byte("v1")},
		{Name: []byte("t2"), Value: []byte("v2")},
	}
	now = time.Now()
)

func makeTagIter() ident.TagIterator {
	ts := models.EmptyTags().AddTags(testTags)
	return TagsToIdentTagIterator(ts)
}

func TestTagsToIdentTagIterator(t *testing.T) {
	tagIter := makeTagIter()
	defer tagIter.Close()

	tags := make([]models.Tag, len(testTags))
	for i := 0; tagIter.Next(); i++ {
		tags[i] = models.Tag{
			Name:  tagIter.Current().Name.Bytes(),
			Value: tagIter.Current().Value.Bytes(),
		}
	}

	assert.Equal(t, testTags, tags)
}

func TestFromM3IdentToMetric(t *testing.T) {
	tagIters := makeTagIter()
	name := []byte("foobarbaz")
	metric, err := FromM3IdentToMetric(testID, tagIters, models.NewTagOptions().SetMetricName(name))
	require.NoError(t, err)

	assert.Equal(t, testID.String(), metric.ID)
	assert.Equal(t, testTags, metric.Tags.Tags)
	assert.Equal(t, name, metric.Tags.Opts.GetMetricName())
}

func TestFromIdentTagIteratorToTags(t *testing.T) {
	tagIters := makeTagIter()
	tags, err := FromIdentTagIteratorToTags(tagIters, nil)
	require.NoError(t, err)
	require.Equal(t, len(testTags), tags.Len())
	assert.Equal(t, testTags, tags.Tags)
	assert.Equal(t, []byte("__name__"), tags.Opts.GetMetricName())
}

func TestFetchQueryToM3Query(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		matchers models.Matchers
	}{
		{
			name:     "exact match",
			expected: "conjunction(term(t1, v1))",
			matchers: models.Matchers{
				{
					Type:  models.MatchEqual,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "exact match negated",
			expected: "conjunction(negation(term(t1, v1)))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotEqual,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "regexp match",
			expected: "conjunction(regexp(t1, v1))",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "regexp match negated",
			expected: "conjunction(negation(regexp(t1, v1)))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotRegexp,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fetchQuery := &FetchQuery{
				Raw:         "up",
				TagMatchers: test.matchers,
				Start:       now.Add(-5 * time.Minute),
				End:         now,
				Interval:    15 * time.Second,
			}

			m3Query, err := FetchQueryToM3Query(fetchQuery)
			require.NoError(t, err)
			assert.Equal(t, test.expected, m3Query.String())
		})
	}

}
