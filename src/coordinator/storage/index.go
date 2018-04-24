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
	"github.com/m3db/m3coordinator/models"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
)

// FromM3IdentToMetric converts an M3 ident metric to a coordinator metric
func FromM3IdentToMetric(identNamespace, identID ident.ID, iterTags ident.Tags) *models.Metric {
	namespace := identNamespace.String()
	id := identID.String()
	tags := FromIdentTagsToTags(iterTags)

	return &models.Metric{
		ID:        id,
		Namespace: namespace,
		Tags:      tags,
	}
}

// FromIdentTagsToTags converts ident tags to coordinator tags
func FromIdentTagsToTags(identTags ident.Tags) models.Tags {
	tags := make(models.Tags, len(identTags))
	for _, identTag := range identTags {
		tags[identTag.Name.String()] = identTag.Value.String()
		identTag.Finalize()
	}
	return tags
}

// FetchOptionsToM3Options converts a set of coordinator options to M3 options
func FetchOptionsToM3Options(fetchOptions *FetchOptions, fetchQuery *FetchQuery) index.QueryOptions {
	return index.QueryOptions{
		Limit:          fetchOptions.Limit,
		StartInclusive: fetchQuery.Start,
		EndExclusive:   fetchQuery.End,
	}
}

// FetchQueryToM3Query converts an m3coordinator fetch query to an M3 query
func FetchQueryToM3Query(fetchQuery *FetchQuery) index.Query {
	var indexQuery index.Query
	segQuery := segment.Query{
		Filters:     MatchersToFilters(fetchQuery.TagMatchers),
		Conjunction: segment.AndConjunction, // NB (braskin): & is the only conjunction supported currently
	}
	indexQuery.Query = segQuery

	return indexQuery
}

// MatchersToFilters converts matchers to M3 filters
func MatchersToFilters(matchers models.Matchers) []segment.Filter {
	var (
		filters []segment.Filter
		negate  bool
		regexp  bool
	)

	for _, matcher := range matchers {
		if matcher.Type == models.MatchNotEqual || matcher.Type == models.MatchNotRegexp {
			negate = true
		}
		if matcher.Type == models.MatchNotRegexp || matcher.Type == models.MatchRegexp {
			regexp = true
		}

		filters = append(filters, segment.Filter{
			FieldName:        []byte(matcher.Name),
			FieldValueFilter: []byte(matcher.Value),
			Negate:           negate,
			Regexp:           regexp,
		})
	}
	return filters
}
