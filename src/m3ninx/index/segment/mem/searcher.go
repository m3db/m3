// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"
)

var (
	errFirstFilterMustNotBeNegation = errors.New("first filter must be non-negation")
	errEmptyQuery                   = errors.New("empty query specified")
)

type negationMergeFn func(x segment.PostingsList, y segment.ImmutablePostingsList) error

func differenceNegationFn(x segment.PostingsList, y segment.ImmutablePostingsList) error {
	return x.Difference(y)
}
func unionNegationFn(x segment.PostingsList, y segment.ImmutablePostingsList) error {
	return x.Union(y)
}

type sequentialSearcher struct {
	queryable       queryable
	negationMergeFn negationMergeFn
	pool            segment.PostingsListPool
}

// newSequentialSearcher returns a new sequential searcher.
func newSequentialSearcher(
	q queryable,
	negationMergeFn negationMergeFn,
	pool segment.PostingsListPool,
) searcher {
	return &sequentialSearcher{
		queryable:       q,
		negationMergeFn: negationMergeFn,
		pool:            pool,
	}
}

// NB: the current implementation of Query() modifies the input arg, which
// makes it un-safe for concurrent callers. We should address this if we
// we start fetching filters in parallel.
func (s *sequentialSearcher) Query(query segment.Query) (
	candidateDocIDs segment.PostingsList,
	pendingFilterFn matchPredicate,
	err error,
) {
	if err := validateQuery(query); err != nil {
		return nil, nil, err
	}

	// order filters to ensure the first filter has no-negation
	filters := orderFiltersByNonNegated(query.Filters)
	sort.Sort(filters)

	var (
		candidateDocIds segment.PostingsList
		predicates      = make([]matchPredicate, 0, len(query.Filters))
	)
	// TODO: support parallel fetching across segments/filters
	for filterIdx, filter := range query.Filters {
		if filterIdx == 0 && filter.Negate {
			return nil, nil, errFirstFilterMustNotBeNegation
		}

		fetchedIds, pred, err := s.queryable.Filter(filter)
		if err != nil {
			return nil, nil, err
		}

		if pred != nil {
			predicates = append(predicates, pred)
		}

		// i.e. we don't have any documents for the given filter, can early terminate entire fn
		if fetchedIds == nil {
			return nil, nil, nil
		}

		if candidateDocIds == nil {
			candidateDocIds = fetchedIds
			continue
		}

		// TODO: evaluate perf impact of retrieving all candidate docIDs, waiting till end,
		// sorting by size and then doing the intersection

		// update candidate set
		var mergeErr error
		if filter.Negate {
			mergeErr = s.negationMergeFn(candidateDocIds, fetchedIds)
		} else {
			mergeErr = candidateDocIds.Intersect(fetchedIds)
		}

		if mergeErr != nil {
			return nil, nil, mergeErr
		}

		// early terminate if we don't have any docs in candidate set
		if candidateDocIds.IsEmpty() {
			return nil, nil, nil
		}
	}

	// TODO: once we support multiple segments, we'll have to merge results
	return candidateDocIds, matchPredicates(predicates).Fn(), nil
}

type matchPredicates []matchPredicate

func (m matchPredicates) Fn() matchPredicate {
	return func(d doc.Document) bool {
		for _, fn := range m {
			if !fn(d) {
				return false
			}
		}
		return true
	}
}

// orderFiltersByNonNegated orders filters which are not negated first in the list.
type orderFiltersByNonNegated []segment.Filter

func (of orderFiltersByNonNegated) Len() int           { return len(of) }
func (of orderFiltersByNonNegated) Swap(i, j int)      { of[i], of[j] = of[j], of[i] }
func (of orderFiltersByNonNegated) Less(i, j int) bool { return !of[i].Negate && of[j].Negate }

// validate any assumptions we have about queries.
func validateQuery(q segment.Query) error {
	// assuming we only support AndConjuctions for now.
	if q.Conjunction != segment.AndConjunction {
		return fmt.Errorf("query: %v has an invalid conjuction: %v", q, q.Conjunction)
	}

	// ensure query level have at-least one filter or sub-query
	if len(q.Filters) == 0 && len(q.SubQueries) == 0 {
		return errEmptyQuery
	}

	// ensure we don't have any level with only Negations as they are super expensive to compute
	if len(q.Filters) != 0 {
		hasNonNegationFilter := false
		for _, f := range q.Filters {
			if !f.Negate {
				hasNonNegationFilter = true
				break
			}
		}
		if !hasNonNegationFilter {
			return fmt.Errorf("query: %v has only negation filters, specify at least one non-negation filter", q)
		}
	}

	// ensure all sub-queries are valid too
	for _, sub := range q.SubQueries {
		if err := validateQuery(sub); err != nil {
			return err
		}
	}

	// all good
	return nil
}
