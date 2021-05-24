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
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	dot  = byte('.')
	plus = byte('+')
	star = byte('*')
)

// FromM3IdentToMetric converts an M3 ident metric to a coordinator metric.
func FromM3IdentToMetric(
	identID ident.ID,
	iterTags ident.TagIterator,
	tagOptions models.TagOptions,
) (models.Metric, error) {
	tags, err := consolidators.FromIdentTagIteratorToTags(iterTags, tagOptions)
	if err != nil {
		return models.Metric{}, err
	}

	return models.Metric{
		ID:   identID.Bytes(),
		Tags: tags,
	}, nil
}

// TagsToIdentTagIterator converts coordinator tags to ident tags.
func TagsToIdentTagIterator(tags models.Tags) ident.TagIterator {
	// TODO: get a tags and tag iterator from an ident.Pool here rather than allocing them here
	identTags := make([]ident.Tag, 0, tags.Len())
	for _, t := range tags.Tags {
		identTags = append(identTags, ident.Tag{
			Name:  ident.BytesID(t.Name),
			Value: ident.BytesID(t.Value),
		})
	}

	return ident.NewTagsIterator(ident.NewTags(identTags...))
}

// FetchOptionsToM3Options converts a set of coordinator options to M3 options.
func FetchOptionsToM3Options(fetchOptions *FetchOptions, fetchQuery *FetchQuery) index.QueryOptions {
	return index.QueryOptions{
		SeriesLimit:       fetchOptions.SeriesLimit,
		DocsLimit:         fetchOptions.DocsLimit,
		RequireExhaustive: fetchOptions.RequireExhaustive,
		RequireNoWait:     fetchOptions.RequireNoWait,
		Source:            fetchOptions.Source,
		StartInclusive:    xtime.ToUnixNano(fetchQuery.Start),
		EndExclusive:      xtime.ToUnixNano(fetchQuery.End),
	}
}

func convertAggregateQueryType(completeNameOnly bool) index.AggregationType {
	if completeNameOnly {
		return index.AggregateTagNames
	}

	return index.AggregateTagNamesAndValues
}

// FetchOptionsToAggregateOptions converts a set of coordinator options as well
// as complete tags query to an M3 aggregate query option.
func FetchOptionsToAggregateOptions(
	fetchOptions *FetchOptions,
	tagQuery *CompleteTagsQuery,
) index.AggregationOptions {
	return index.AggregationOptions{
		QueryOptions: index.QueryOptions{
			SeriesLimit:       fetchOptions.SeriesLimit,
			DocsLimit:         fetchOptions.DocsLimit,
			Source:            fetchOptions.Source,
			RequireExhaustive: fetchOptions.RequireExhaustive,
			RequireNoWait:     fetchOptions.RequireNoWait,
			StartInclusive:    tagQuery.Start,
			EndExclusive:      tagQuery.End,
		},
		FieldFilter: tagQuery.FilterNameTags,
		Type:        convertAggregateQueryType(tagQuery.CompleteNameOnly),
	}
}

// FetchQueryToM3Query converts an m3coordinator fetch query to an M3 query.
func FetchQueryToM3Query(
	fetchQuery *FetchQuery,
	options *FetchOptions,
) (index.Query, error) {
	fetchQuery = fetchQuery.WithAppliedOptions(options)
	matchers := fetchQuery.TagMatchers
	// If no matchers provided, explicitly set this to an AllQuery.
	if len(matchers) == 0 {
		return index.Query{
			Query: idx.NewAllQuery(),
		}, nil
	}

	// Optimization for single matcher case.
	if len(matchers) == 1 {
		specialCase := isSpecialCaseMatcher(matchers[0])
		if specialCase.skip {
			// NB: only matcher has no effect; this is synonymous to an AllQuery.
			return index.Query{
				Query: idx.NewAllQuery(),
			}, nil
		}

		if specialCase.isSpecial {
			return index.Query{Query: specialCase.query}, nil
		}

		q, err := matcherToQuery(matchers[0])
		if err != nil {
			return index.Query{}, err
		}

		return index.Query{Query: q}, nil
	}

	idxQueries := make([]idx.Query, 0, len(matchers))
	for _, matcher := range matchers {
		specialCase := isSpecialCaseMatcher(matcher)
		if specialCase.skip {
			continue
		}

		if specialCase.isSpecial {
			idxQueries = append(idxQueries, specialCase.query)
			continue
		}

		q, err := matcherToQuery(matcher)
		if err != nil {
			return index.Query{}, err
		}

		idxQueries = append(idxQueries, q)
	}

	q := idx.NewConjunctionQuery(idxQueries...)

	return index.Query{Query: q}, nil
}

type specialCase struct {
	query     idx.Query
	isSpecial bool
	skip      bool
}

func isSpecialCaseMatcher(matcher models.Matcher) specialCase {
	if len(matcher.Value) == 0 {
		if matcher.Type == models.MatchNotRegexp ||
			matcher.Type == models.MatchNotEqual {
			query := idx.NewFieldQuery(matcher.Name)
			return specialCase{query: query, isSpecial: true}
		}

		if matcher.Type == models.MatchRegexp ||
			matcher.Type == models.MatchEqual {
			query := idx.NewNegationQuery(idx.NewFieldQuery(matcher.Name))
			return specialCase{query: query, isSpecial: true}
		}

		return specialCase{}
	}

	// NB: no special case for regex / not regex here.
	isNegatedRegex := matcher.Type == models.MatchNotRegexp
	isRegex := matcher.Type == models.MatchRegexp
	if !isNegatedRegex && !isRegex {
		return specialCase{}
	}

	if len(matcher.Value) != 2 || matcher.Value[0] != dot {
		return specialCase{}
	}

	if matcher.Value[1] == star {
		if isNegatedRegex {
			// NB: This should match no results.
			query := idx.NewNegationQuery(idx.NewAllQuery())
			return specialCase{query: query, isSpecial: true}
		}

		// NB: this matcher should not affect query results.
		return specialCase{skip: true}
	}

	if matcher.Value[1] == plus {
		query := idx.NewFieldQuery(matcher.Name)
		if isNegatedRegex {
			query = idx.NewNegationQuery(query)
		}

		return specialCase{query: query, isSpecial: true}
	}

	return specialCase{}
}

func matcherToQuery(matcher models.Matcher) (idx.Query, error) {
	negate := false
	switch matcher.Type {
	// Support for Regexp types
	case models.MatchNotRegexp:
		negate = true
		fallthrough

	case models.MatchRegexp:
		var (
			query idx.Query
			err   error
		)

		query, err = idx.NewRegexpQuery(matcher.Name, matcher.Value)
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

	case models.MatchNotField:
		negate = true
		fallthrough

	case models.MatchField:
		query := idx.NewFieldQuery(matcher.Name)
		if negate {
			query = idx.NewNegationQuery(query)
		}

		return query, nil

	case models.MatchAll:
		return idx.NewAllQuery(), nil

	default:
		return idx.Query{}, fmt.Errorf("unsupported query type: %v", matcher)
	}
}
