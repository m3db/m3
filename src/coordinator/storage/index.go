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

	"github.com/m3db/m3db/src/coordinator/models"

	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3x/ident"
)

// FromM3IdentToMetric converts an M3 ident metric to a coordinator metric
func FromM3IdentToMetric(identNamespace, identID ident.ID, iterTags ident.TagIterator) (*models.Metric, error) {
	namespace := identNamespace.String()
	id := identID.String()
	tags, err := FromIdentTagIteratorToTags(iterTags)
	if err != nil {
		return nil, err
	}

	return &models.Metric{
		ID:        id,
		Namespace: namespace,
		Tags:      tags,
	}, nil
}

// FromIdentTagIteratorToTags converts ident tags to coordinator tags
func FromIdentTagIteratorToTags(identTags ident.TagIterator) (models.Tags, error) {
	tags := make(models.Tags, identTags.Remaining())

	for identTags.Next() {
		identTag := identTags.Current()

		tags[identTag.Name.String()] = identTag.Value.String()
	}

	if err := identTags.Err(); err != nil {
		return nil, err
	}

	return tags, nil
}

// TagsToIdentTagIterator converts coordinator tags to ident tags
func TagsToIdentTagIterator(tags models.Tags) ident.TagIterator {
	identTags := make([]ident.Tag, 0, len(tags))

	for key, val := range tags {
		identTags = append(identTags, ident.StringTag(key, val))
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

func matcherToQuery(matcher *models.Matcher) (idx.Query, error) {
	switch matcher.Type {
	case models.MatchRegexp:
		return idx.NewRegexpQuery([]byte(matcher.Name), []byte(matcher.Value))

	case models.MatchEqual:
		return idx.NewTermQuery([]byte(matcher.Name), []byte(matcher.Value)), nil

	default:
		return idx.Query{}, fmt.Errorf("unsupported query type %v", matcher)

	}
}
