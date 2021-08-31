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
	"testing"

	"github.com/m3db/m3/src/m3ninx/search"

	"github.com/stretchr/testify/require"
)

func TestConjunctionQuery(t *testing.T) {
	tests := []struct {
		name    string
		queries []search.Query
	}{
		{
			name: "no queries provided",
		},
		{
			name: "a single query provided",
			queries: []search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
			},
		},
		{
			name: "multiple queries provided",
			queries: []search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewTermQuery([]byte("vegetable"), []byte("carrot")),
			},
		},
		{
			name: "multiple queries including negations",
			queries: []search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewTermQuery([]byte("vegetable"), []byte("carrot")),
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("banana"))),
			},
		},
		{
			name: "single negation query",
			queries: []search.Query{
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("banana"))),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewConjunctionQuery(test.queries)
			_, err := q.Searcher()
			require.NoError(t, err)
		})
	}
}

func TestConjunctionQueryEqual(t *testing.T) {
	tests := []struct {
		name        string
		left, right search.Query
		expected    bool
	}{
		{
			name:     "empty queries",
			left:     NewConjunctionQuery(nil),
			right:    NewConjunctionQuery(nil),
			expected: true,
		},
		{
			name: "equal queries",
			left: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("banana"))),
			}),
			right: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("banana"))),
			}),
			expected: true,
		},
		{
			name: "single query",
			left: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
			}),
			right:    NewTermQuery([]byte("fruit"), []byte("apple")),
			expected: true,
		},
		{
			name: "different order",
			left: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewTermQuery([]byte("fruit"), []byte("banana")),
			}),
			right: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("banana")),
				NewTermQuery([]byte("fruit"), []byte("apple")),
			}),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.left.Equal(test.right))
		})
	}
}
