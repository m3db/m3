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
	"bytes"
	"fmt"

	"github.com/m3db/m3ninx/generated/proto/querypb"
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/searcher"
)

// TermQuery finds document which match the given term exactly.
type TermQuery struct {
	Field []byte
	Term  []byte
}

// NewTermQuery constructs a new TermQuery for the given field and term.
func NewTermQuery(field, term []byte) search.Query {
	return &TermQuery{
		Field: field,
		Term:  term,
	}
}

// Searcher returns a searcher over the provided readers.
func (q *TermQuery) Searcher(rs index.Readers) (search.Searcher, error) {
	return searcher.NewTermSearcher(rs, q.Field, q.Term), nil
}

// Equal reports whether q is equivalent to o.
func (q *TermQuery) Equal(o search.Query) bool {
	o, ok := singular(o)
	if !ok {
		return false
	}

	inner, ok := o.(*TermQuery)
	if !ok {
		return false
	}

	return bytes.Equal(q.Field, inner.Field) && bytes.Equal(q.Term, inner.Term)
}

// ToProto returns the Protobuf query struct corresponding to the term query.
func (q *TermQuery) ToProto() *querypb.Query {
	term := querypb.TermQuery{
		Field: q.Field,
		Term:  q.Term,
	}

	return &querypb.Query{
		Query: &querypb.Query_Term{Term: &term},
	}
}

func (q *TermQuery) String() string {
	return fmt.Sprintf("term(%s, %s)", q.Field, q.Term)
}
