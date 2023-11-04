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
	"bytes"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/regexp"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	dotStar = []byte(".*")
	dotPlus = []byte(".+")
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
func FetchOptionsToM3Options(
	fetchOptions *FetchOptions,
	fetchQuery *FetchQuery,
) (index.QueryOptions, error) {
	start, end, err := convertStartEndWithRangeLimit(fetchQuery.Start,
		fetchQuery.End, fetchOptions)
	if err != nil {
		return index.QueryOptions{}, err
	}

	return index.QueryOptions{
		SeriesLimit:                   fetchOptions.SeriesLimit,
		InstanceMultiple:              fetchOptions.InstanceMultiple,
		DocsLimit:                     fetchOptions.DocsLimit,
		RequireExhaustive:             fetchOptions.RequireExhaustive,
		RequireNoWait:                 fetchOptions.RequireNoWait,
		ReadConsistencyLevel:          fetchOptions.ReadConsistencyLevel,
		IterateEqualTimestampStrategy: fetchOptions.IterateEqualTimestampStrategy,
		Source:                        fetchOptions.Source,
		StartInclusive:                xtime.ToUnixNano(start),
		EndExclusive:                  xtime.ToUnixNano(end),
	}, nil
}

func convertStartEndWithRangeLimit(
	start, end time.Time,
	fetchOptions *FetchOptions,
) (time.Time, time.Time, error) {
	fetchRangeLimit := fetchOptions.RangeLimit
	if fetchRangeLimit <= 0 {
		return start, end, nil
	}

	fetchRange := end.Sub(start)
	if fetchRange <= fetchRangeLimit {
		return start, end, nil
	}

	if fetchOptions.RequireExhaustive {
		// Fail the query.
		msg := fmt.Sprintf("query exceeded limit: require_exhaustive=%v, "+
			"range_limit=%s, range_matched=%s",
			fetchOptions.RequireExhaustive,
			fetchRangeLimit.String(),
			fetchRange.String())
		err := xerrors.NewInvalidParamsError(consolidators.NewLimitError(msg))
		return time.Time{}, time.Time{}, err
	}

	// Truncate the range.
	start = end.Add(-1 * fetchRangeLimit)
	return start, end, nil
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
) (index.AggregationOptions, error) {
	start, end, err := convertStartEndWithRangeLimit(tagQuery.Start.ToTime(),
		tagQuery.End.ToTime(), fetchOptions)
	if err != nil {
		return index.AggregationOptions{}, err
	}

	return index.AggregationOptions{
		QueryOptions: index.QueryOptions{
			SeriesLimit:       fetchOptions.SeriesLimit,
			DocsLimit:         fetchOptions.DocsLimit,
			Source:            fetchOptions.Source,
			RequireExhaustive: fetchOptions.RequireExhaustive,
			RequireNoWait:     fetchOptions.RequireNoWait,
			StartInclusive:    xtime.ToUnixNano(start),
			EndExclusive:      xtime.ToUnixNano(end),
		},
		FieldFilter: tagQuery.FilterNameTags,
		Type:        convertAggregateQueryType(tagQuery.CompleteNameOnly),
	}, nil
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
		specialCase, err := isSpecialCaseMatcher(matchers[0])
		if err != nil {
			return index.Query{}, err
		}
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
		specialCase, err := isSpecialCaseMatcher(matcher)
		if err != nil {
			return index.Query{}, err
		}
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

func isSpecialCaseMatcher(matcher models.Matcher) (specialCase, error) {
	if len(matcher.Value) == 0 {
		if matcher.Type == models.MatchNotRegexp ||
			matcher.Type == models.MatchNotEqual {
			query := idx.NewFieldQuery(matcher.Name)
			return specialCase{query: query, isSpecial: true}, nil
		}

		if matcher.Type == models.MatchRegexp ||
			matcher.Type == models.MatchEqual {
			query := idx.NewNegationQuery(idx.NewFieldQuery(matcher.Name))
			return specialCase{query: query, isSpecial: true}, nil
		}

		return specialCase{}, nil
	}

	// NB: no special case except for regex / notRegex here.
	isNegatedRegex := matcher.Type == models.MatchNotRegexp
	isRegex := matcher.Type == models.MatchRegexp
	if !isNegatedRegex && !isRegex {
		return specialCase{}, nil
	}

	if bytes.Equal(matcher.Value, dotStar) {
		if isNegatedRegex {
			// NB: This should match no results.
			query := idx.NewNegationQuery(idx.NewAllQuery())
			return specialCase{query: query, isSpecial: true}, nil
		}

		// NB: this matcher should not affect query results.
		return specialCase{skip: true}, nil
	}

	if bytes.Equal(matcher.Value, dotPlus) {
		query := idx.NewFieldQuery(matcher.Name)
		if isNegatedRegex {
			query = idx.NewNegationQuery(query)
		}

		return specialCase{query: query, isSpecial: true}, nil
	}

	matchesEmpty, err := regexp.MatchesEmptyValue(matcher.Value)
	if err != nil {
		return specialCase{}, regexError(err)
	}

	if matchesEmpty {
		regexpQuery, err := idx.NewRegexpQuery(matcher.Name, matcher.Value)
		if err != nil {
			return specialCase{}, err
		}

		if isNegatedRegex {
			return specialCase{
				query: idx.NewConjunctionQuery(
					idx.NewNegationQuery(regexpQuery),
					idx.NewFieldQuery(matcher.Name),
				),
				isSpecial: true,
			}, nil
		}

		return specialCase{
			query: idx.NewDisjunctionQuery(
				regexpQuery,
				idx.NewNegationQuery(idx.NewFieldQuery(matcher.Name)),
			),
			isSpecial: true,
		}, nil
	}

	return specialCase{}, nil
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

func regexError(err error) error {
	return xerrors.NewInvalidParamsError(xerrors.Wrap(err, "regex error"))
}
