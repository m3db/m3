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

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

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

	assert.Equal(t, testID.Bytes(), metric.ID)
	assert.Equal(t, testTags, metric.Tags.Tags)
	assert.Equal(t, name, metric.Tags.Opts.MetricName())
}

func TestFetchQueryToM3Query(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		matchers models.Matchers
	}{
		{
			name:     "exact match",
			expected: "term(t1, v1)",
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
			expected: "negation(term(t1, v1))",
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
			expected: "regexp(t1, v1)",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "regexp match dot star -> all",
			expected: "all()",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte(".*"),
				},
			},
		},
		{
			name:     "regexp match dot plus -> field",
			expected: "field(t1)",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte(".+"),
				},
			},
		},
		{
			name:     "regexp match negated",
			expected: "negation(regexp(t1, v1))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotRegexp,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "regexp match negated",
			expected: "negation(all())",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotRegexp,
					Name:  []byte("t1"),
					Value: []byte(".*"),
				},
			},
		},
		{
			name:     "field match",
			expected: "field(t1)",
			matchers: models.Matchers{
				{
					Type:  models.MatchField,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "field match negated",
			expected: "negation(field(t1))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotField,
					Name:  []byte("t1"),
					Value: []byte("v1"),
				},
			},
		},
		{
			name:     "all matchers",
			expected: "all()",
			matchers: models.Matchers{},
		},
		{
			name:     "all matchers",
			expected: "all()",
			matchers: models.Matchers{
				{
					Type: models.MatchAll,
				},
			},
		},
		{
			name:     "regexp match dot star with trailing characters -> regex",
			expected: "regexp(t1, .*foo)",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte(".*foo"),
				},
			},
		},
		{
			name:     "regexp match dot plus with trailing characters -> regex",
			expected: "regexp(t1, .+foo)",
			matchers: models.Matchers{
				{
					Type:  models.MatchRegexp,
					Name:  []byte("t1"),
					Value: []byte(".+foo"),
				},
			},
		},
		{
			name:     "not regexp match dot star with trailing characters -> regex",
			expected: "negation(regexp(t1, .*foo))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotRegexp,
					Name:  []byte("t1"),
					Value: []byte(".*foo"),
				},
			},
		},
		{
			name:     "not regexp match dot plus with trailing characters -> regex",
			expected: "negation(regexp(t1, .+foo))",
			matchers: models.Matchers{
				{
					Type:  models.MatchNotRegexp,
					Name:  []byte("t1"),
					Value: []byte(".+foo"),
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

			m3Query, err := FetchQueryToM3Query(fetchQuery, nil)
			require.NoError(t, err)
			assert.Equal(t, test.expected, m3Query.String())
		})
	}
}

func TestFetchOptionsToAggregateOptions(t *testing.T) {
	fetchOptions := &FetchOptions{
		SeriesLimit:       7,
		DocsLimit:         8,
		RequireExhaustive: true,
	}

	end := xtime.Now()
	start := end.Add(-1 * time.Hour)
	filter := [][]byte{[]byte("filter")}
	matchers := models.Matchers{
		models.Matcher{Type: models.MatchNotRegexp,
			Name: []byte("foo"), Value: []byte("bar")},
	}

	tagQuery := &CompleteTagsQuery{
		Start:            start,
		End:              end,
		TagMatchers:      matchers,
		FilterNameTags:   filter,
		CompleteNameOnly: true,
	}

	aggOpts := FetchOptionsToAggregateOptions(fetchOptions, tagQuery)
	assert.Equal(t, end, aggOpts.EndExclusive)
	assert.Equal(t, start, aggOpts.StartInclusive)
	assert.Equal(t, index.AggregateTagNames, aggOpts.Type)
	require.Equal(t, 1, len(aggOpts.FieldFilter))
	require.Equal(t, "filter", string(aggOpts.FieldFilter[0]))
	require.Equal(t, fetchOptions.SeriesLimit, aggOpts.SeriesLimit)
	require.Equal(t, fetchOptions.DocsLimit, aggOpts.DocsLimit)
	require.Equal(t, fetchOptions.RequireExhaustive, aggOpts.RequireExhaustive)
}
