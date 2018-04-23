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

package idx

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/query"

	"github.com/golang/mock/gomock"
)

// QueryMatcher is a gomock.Matcher that matches idx.Query
type QueryMatcher interface {
	gomock.Matcher
}

// NewQueryMatcher returns a new QueryMatcher
func NewQueryMatcher(q Query) QueryMatcher {
	return &queryMatcher{
		query: q,
	}
}

type queryMatcher struct {
	query Query
}

func (m *queryMatcher) Matches(x interface{}) bool {
	query, ok := x.(Query)
	if !ok {
		return false
	}

	expected := m.query.SearchQuery()
	observed := query.SearchQuery()
	return m.matches(expected, observed)
}

func (m *queryMatcher) headElem(qry search.Query) (search.Query, bool) {
	switch q := qry.(type) {
	case *query.TermQuery:
		return q, true
	case *query.RegexpQuery:
		return q, true
	case *query.ConjuctionQuery:
		if len(q.Queries) == 1 {
			return q.Queries[0], true
		}
	case *query.DisjuctionQuery:
		if len(q.Queries) == 1 {
			return q.Queries[0], true
		}
	}
	return nil, false
}

func (m *queryMatcher) matches(expected, observed search.Query) bool {
	switch q := expected.(type) {
	case *query.TermQuery:
		oq, ok := m.headElem(observed)
		if !ok {
			return false
		}
		term, ok := oq.(*query.TermQuery)
		if !ok {
			return false
		}
		if !bytes.Equal(q.Field, term.Field) ||
			!bytes.Equal(q.Term, term.Term) {
			return false
		}
	case *query.RegexpQuery:
		oq, ok := m.headElem(observed)
		if !ok {
			return false
		}
		req, ok := oq.(*query.RegexpQuery)
		if !ok {
			return false
		}
		if !bytes.Equal(q.Field, req.Field) ||
			!bytes.Equal(q.Regexp, req.Regexp) {
			return false
		}
	case *query.ConjuctionQuery:
		conj, ok := observed.(*query.ConjuctionQuery)
		if !ok || len(q.Queries) != len(conj.Queries) {
			return false
		}
		for idx := range q.Queries {
			exp, obs := q.Queries[idx], conj.Queries[idx]
			if ok := m.matches(exp, obs); !ok {
				return false
			}
		}
	case *query.DisjuctionQuery:
		disj, ok := observed.(*query.DisjuctionQuery)
		if !ok || len(q.Queries) != len(disj.Queries) {
			return false
		}
		for idx := range q.Queries {
			exp, obs := q.Queries[idx], disj.Queries[idx]
			if ok := m.matches(exp, obs); !ok {
				return false
			}
		}
	default:
		panic("unknown type")
	}
	return true
}

func (m *queryMatcher) String() string {
	return fmt.Sprintf("QueryMatcher %v", m.query)
}
