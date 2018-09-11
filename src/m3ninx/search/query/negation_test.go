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

func TestNegationQuery(t *testing.T) {
	tests := []struct {
		name  string
		query search.Query
	}{
		{
			name:  "valid query",
			query: NewTermQuery([]byte("fruit"), []byte("apple")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewNegationQuery(test.query)
			_, err := q.Searcher()
			require.NoError(t, err)
		})
	}
}

func TestNegationQueryEqual(t *testing.T) {
	tests := []struct {
		name        string
		left, right search.Query
		expected    bool
	}{
		{
			name:     "same inner queries",
			left:     NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			right:    NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			expected: true,
		},
		{
			name: "singular conjunction query",
			left: NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			right: NewConjunctionQuery([]search.Query{
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			}),
			expected: true,
		},
		{
			name: "singular disjunction query",
			left: NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			right: NewDisjunctionQuery([]search.Query{
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			}),
			expected: true,
		},
		{
			name:     "different inner queries",
			left:     NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
			right:    NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("banana"))),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.left.Equal(test.right))
		})
	}
}
