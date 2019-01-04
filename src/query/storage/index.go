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

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3x/ident"
)

// FromM3IdentToMetric converts an M3 ident metric to a coordinator metric
func FromM3IdentToMetric(
	identID ident.ID,
	iterTags ident.TagIterator,
	tagOptions models.TagOptions,
) (models.Metric, error) {
	id := identID.String()
	tags, err := FromIdentTagIteratorToTags(iterTags, tagOptions)
	if err != nil {
		return models.Metric{}, err
	}

	return models.Metric{
		ID:   id,
		Tags: tags,
	}, nil
}

// FromIdentTagIteratorToTags converts ident tags to coordinator tags
func FromIdentTagIteratorToTags(
	identTags ident.TagIterator,
	tagOptions models.TagOptions,
) (models.Tags, error) {
	tags := models.NewTags(identTags.Remaining(), tagOptions)
	for identTags.Next() {
		identTag := identTags.Current()
		tags = tags.AddTag(models.Tag{
			Name:  identTag.Name.Bytes(),
			Value: identTag.Value.Bytes(),
		})
	}

	if err := identTags.Err(); err != nil {
		return models.EmptyTags(), err
	}

	return tags, nil
}

// TagsToIdentTagIterator converts coordinator tags to ident tags
func TagsToIdentTagIterator(tags models.Tags) ident.TagIterator {
	//TODO get a tags and tag iterator from an ident.Pool here rather than allocing them here
	identTags := make([]ident.Tag, 0, tags.Len())
	for _, t := range tags.Tags {
		identTags = append(identTags, ident.Tag{
			Name:  ident.BytesID(t.Name),
			Value: ident.BytesID(t.Value),
		})
	}

	return ident.NewTagsIterator(ident.NewTags(identTags...))
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
func FetchQueryToM3Query(fetchQuery *FetchQuery) (index.Query, error) {
	matchers := fetchQuery.TagMatchers
	idxQueries := make([]idx.Query, len(matchers))
	var err error
	for i, matcher := range matchers {
		idxQueries[i], err = matcherToQuery(matcher)
		if err != nil {
			return index.Query{}, err
		}
	}

	q := idx.NewConjunctionQuery(idxQueries...)
	return index.Query{Query: q}, nil
}

func matcherToQuery(matcher models.Matcher) (idx.Query, error) {
	negate := false
	switch matcher.Type {
	// Support for Regexp types
	case models.MatchNotRegexp:
		negate = true
		fallthrough
	case models.MatchRegexp:
		query, err := idx.NewRegexpQuery(matcher.Name, matcher.Value)
		if err != nil {
			return idx.Query{}, err
		}
		if negate {
			query = idx.NewNegationQuery(query)
		}
		return query, nil

		// Support exact matches
	case models.MatchNotEqual:
		negate = true
		fallthrough
	case models.MatchEqual:
		query := idx.NewTermQuery(matcher.Name, matcher.Value)
		if negate {
			query = idx.NewNegationQuery(query)
		}
		return query, nil

	default:
		return idx.Query{}, fmt.Errorf("unsupported query type: %v", matcher)
	}
}
