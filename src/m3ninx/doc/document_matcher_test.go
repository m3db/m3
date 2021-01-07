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

package doc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocumentMatcher(t *testing.T) {
	tests := []struct {
		name     string
		l, r     Metadata
		expected bool
	}{
		{
			name:     "empty documents are equal",
			l:        Metadata{},
			r:        Metadata{},
			expected: true,
		},
		{
			name: "documents with the same fields in the same order are equal",
			l: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			r: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			expected: true,
		},
		{
			name: "documents with the same fields in different order are unequal",
			l: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			r: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			expected: false,
		},
		{
			name: "documents with different fields are unequal",
			l: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			r: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  []byte("carrot"),
						Value: []byte("orange"),
					},
				},
			},
			expected: false,
		},
		{
			name: "documents with different IDs are unequal",
			l: Metadata{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			r: Metadata{
				ID: []byte("080292"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		require.Equal(t, NewDocumentMatcher(test.l).Matches(test.r), test.expected, test.name)
	}
}
