// Copyright (c) 2019 Uber Technologies, Inc.
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

func TestFieldQuery(t *testing.T) {
	tests := []struct {
		name  string
		field []byte
	}{
		{
			name:  "valid field should not return an error",
			field: []byte("fruit"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := NewFieldQuery(test.field)
			_, err := q.Searcher()
			require.NoError(t, err)
		})
	}
}

func TestFieldQueryEqual(t *testing.T) {
	tests := []struct {
		name        string
		left, right search.Query
		expected    bool
	}{
		{
			name:     "same field",
			left:     NewFieldQuery([]byte("fruit")),
			right:    NewFieldQuery([]byte("fruit")),
			expected: true,
		},
		{
			name: "singular conjunction query",
			left: NewFieldQuery([]byte("fruit")),
			right: NewConjunctionQuery([]search.Query{
				NewFieldQuery([]byte("fruit")),
			}),
			expected: true,
		},
		{
			name: "singular disjunction query",
			left: NewFieldQuery([]byte("fruit")),
			right: NewDisjunctionQuery([]search.Query{
				NewFieldQuery([]byte("fruit")),
			}),
			expected: true,
		},
		{
			name:     "different field",
			left:     NewFieldQuery([]byte("fruit")),
			right:    NewFieldQuery([]byte("food")),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.left.Equal(test.right))
		})
	}
}
