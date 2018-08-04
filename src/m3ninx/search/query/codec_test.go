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

func TestMarshal(t *testing.T) {
	tests := []struct {
		name  string
		query search.Query
	}{
		{
			name:  "term query",
			query: NewTermQuery([]byte("fruit"), []byte("apple")),
		},
		{
			name:  "regexp query",
			query: MustCreateRegexpQuery([]byte("fruit"), []byte(".*ple")),
		},
		{
			name:  "negation query",
			query: NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("apple"))),
		},
		{
			name: "disjunction query",
			query: NewDisjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewConjunctionQuery([]search.Query{
					NewTermQuery([]byte("fruit"), []byte("banana")),
				}),
				NewDisjunctionQuery([]search.Query{
					NewTermQuery([]byte("fruit"), []byte("orange")),
				}),
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("pear"))),
			}),
		},
		{
			name: "conjunction query",
			query: NewConjunctionQuery([]search.Query{
				NewTermQuery([]byte("fruit"), []byte("apple")),
				NewConjunctionQuery([]search.Query{
					NewTermQuery([]byte("fruit"), []byte("banana")),
				}),
				NewDisjunctionQuery([]search.Query{
					NewTermQuery([]byte("fruit"), []byte("orange")),
				}),
				NewNegationQuery(NewTermQuery([]byte("fruit"), []byte("pear"))),
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := Marshal(test.query)
			require.NoError(t, err)

			cpy, err := Unmarshal(data)
			require.NoError(t, err)

			require.True(t, test.query.Equal(cpy))
		})
	}
}

func TestMarshalError(t *testing.T) {
	tests := []struct {
		name  string
		query search.Query
	}{
		{
			name:  "nil query",
			query: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Marshal(test.query)
			require.Error(t, err)
		})
	}
}
