// Copyright (c) 2020 Uber Technologies, Inc.
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

package main

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	series1 = parser.Tags{
		parser.NewTag("foo", "bar"),
		parser.NewTag("baz", "quix"),
	}

	series2 = parser.Tags{
		parser.NewTag("alpha", "a"),
		parser.NewTag("beta", "b"),
	}

	allSeries = list(series1, series2)
)

func TestFilter(t *testing.T) {
	testCases := []struct {
		name          string
		givenMatchers models.Matchers
		wantedSeries  []parser.IngestSeries
	}{
		{
			name:          "No matchers",
			givenMatchers: models.Matchers{},
			wantedSeries:  list(series1, series2),
		},

		{
			name: "MatchEqual on one tag",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchEqual, "bar"),
			},
			wantedSeries: list(series1),
		},
		{
			name: "MatchEqual on two tags",
			givenMatchers: models.Matchers{
				tagMatcher("alpha", models.MatchEqual, "a"),
				tagMatcher("beta", models.MatchEqual, "b"),
			},
			wantedSeries: list(series2),
		},
		{
			name: "Two MatchEqual excluding every series each",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchEqual, "bar"),
				tagMatcher("beta", models.MatchEqual, "b"),
			},
			wantedSeries: list(),
		},
		{
			name: "MatchEqual excluding all series",
			givenMatchers: models.Matchers{
				tagMatcher("unknown", models.MatchEqual, "whatever"),
			},
			wantedSeries: list(),
		},
		{
			name: "MatchEqual on empty value",
			givenMatchers: models.Matchers{
				tagMatcher("unknown", models.MatchEqual, ""),
			},
			wantedSeries: list(series1, series2),
		},

		{
			name: "MatchNotEqual on one tag",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotEqual, "bar"),
			},
			wantedSeries: list(series2),
		},
		{
			name: "MatchNotEqual on two tags",
			givenMatchers: models.Matchers{
				tagMatcher("alpha", models.MatchNotEqual, "a"),
				tagMatcher("beta", models.MatchNotEqual, "b"),
			},
			wantedSeries: list(series1),
		},
		{
			name: "Two MatchNotEqual excluding every series each",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotEqual, "bar"),
				tagMatcher("beta", models.MatchNotEqual, "b"),
			},
			wantedSeries: list(),
		},
		{
			name: "MatchNotEqual accepting all series",
			givenMatchers: models.Matchers{
				tagMatcher("unknown", models.MatchNotEqual, "whatever"),
			},
			wantedSeries: list(series1, series2),
		},
		{
			name: "MatchNotEqual on empty value",
			givenMatchers: models.Matchers{
				tagMatcher("unknown", models.MatchNotEqual, ""),
			},
			wantedSeries: list(),
		},

		{
			name: "MatchRegexp on full value",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchRegexp, "bar"),
			},
			wantedSeries: list(series1),
		},
		{
			name: "MatchRegexp with wildcard",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchRegexp, "b.+"),
			},
			wantedSeries: list(series1),
		},
		{
			name: "MatchRegexp with alternatives",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchRegexp, "bax|bar|baz"),
			},
			wantedSeries: list(series1),
		},
		{
			name: "MatchRegexp unmatched",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchRegexp, "bax|baz"),
			},
			wantedSeries: list(),
		},

		{
			name: "MatchNotRegexp on full value",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotRegexp, "bar"),
			},
			wantedSeries: list(series2),
		},
		{
			name: "MatchNotRegexp with wildcard",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotRegexp, "b.+"),
			},
			wantedSeries: list(series2),
		},
		{
			name: "MatchNotRegexp with alternatives",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotRegexp, "bax|bar|baz"),
			},
			wantedSeries: list(series2),
		},
		{
			name: "MatchNotRegexp matching all",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotRegexp, "bax|baz"),
			},
			wantedSeries: list(series1, series2),
		},

		{
			name: "MatchField",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchField, ""),
			},
			wantedSeries: list(series1),
		},

		{
			name: "MatchNotField",
			givenMatchers: models.Matchers{
				tagMatcher("foo", models.MatchNotField, ""),
			},
			wantedSeries: list(series2),
		},

		{
			name: `Ignore 'role="remote"' matcher added by Prometheus`,
			givenMatchers: models.Matchers{
				tagMatcher("role", models.MatchEqual, "remote"),
			},
			wantedSeries: list(series1, series2),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unfilteredIters, err := toSeriesIterators(allSeries)
			require.NoError(t, err)
			filteredIters := filter(unfilteredIters, tc.givenMatchers)
			filteredSeries := fromSeriesIterators(filteredIters)
			assert.Equal(t, tc.wantedSeries, filteredSeries)
		})
	}
}

func tagMatcher(tag string, matchType models.MatchType, value string) models.Matcher {
	return models.Matcher{
		Type:  matchType,
		Name:  []byte(tag),
		Value: []byte(value),
	}
}

func list(tagsList ...parser.Tags) []parser.IngestSeries {
	list := make([]parser.IngestSeries, 0, len(tagsList))

	for _, tags := range tagsList {
		list = append(list, parser.IngestSeries{Tags: tags})
	}

	return list
}

func toSeriesIterators(series []parser.IngestSeries) (encoding.SeriesIterators, error) {
	return parser.BuildSeriesIterators(
		series, xtime.ToUnixNano(time.Now()), time.Hour, iteratorOpts)
}

func fromSeriesIterators(seriesIters encoding.SeriesIterators) []parser.IngestSeries {
	result := make([]parser.IngestSeries, 0, seriesIters.Len())
	for _, iter := range seriesIters.Iters() {
		tagsIter := iter.Tags()
		tags := make(parser.Tags, 0, tagsIter.Len())
		for tagsIter.Next() {
			tag := tagsIter.Current()
			tags = append(tags, parser.NewTag(tag.Name.String(), tag.Value.String()))
		}
		result = append(result, parser.IngestSeries{Tags: tags})
	}

	return result
}
