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

package query

import (
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/searcher"
)

// conjuctionQuery finds documents which match at least one of the given queries.
type conjuctionQuery struct {
	queries []search.Query
}

// NewConjuctionQuery constructs a new query which matches documents which match all
// of the given queries.
func NewConjuctionQuery(queries []search.Query) search.Query {
	qs := make([]search.Query, 0, len(queries))
	for _, query := range queries {
		// Merge conjunction queries into slice of top-level queries.
		q, ok := query.(*conjuctionQuery)
		if ok {
			qs = append(qs, q.queries...)
			continue
		}

		qs = append(qs, query)
	}
	return &conjuctionQuery{
		queries: qs,
	}
}

func (q *conjuctionQuery) Searcher(rs index.Readers) (search.Searcher, error) {
	switch len(q.queries) {
	case 0:
		return searcher.NewEmptySearcher(len(rs)), nil

	case 1:
		return q.queries[0].Searcher(rs)
	}

	srs := make(search.Searchers, 0, len(q.queries))
	for _, q := range q.queries {
		sr, err := q.Searcher(rs)
		if err != nil {
			return nil, err
		}
		srs = append(srs, sr)
	}

	return searcher.NewConjunctionSearcher(srs)
}
