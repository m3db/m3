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
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortingFields(t *testing.T) {
	tests := []struct {
		name            string
		input, expected Fields
	}{
		{
			name:     "empty list should be unchanged",
			input:    Fields{},
			expected: Fields{},
		},
		{
			name: "sorted fields should remain sorted",
			input: Fields{
				Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
				Field{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
			expected: Fields{
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
		{
			name: "unsorted fields should be sorted",
			input: Fields{
				Field{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
				Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
			expected: Fields{
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input
			sort.Sort(actual)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestDocumentGetField(t *testing.T) {
	tests := []struct {
		name        string
		input       Document
		fieldName   []byte
		expectedOk  bool
		expectedVal []byte
	}{
		{
			name: "get existing field",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			fieldName:   []byte("apple"),
			expectedOk:  true,
			expectedVal: []byte("red"),
		},
		{
			name: "get nonexisting field",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			fieldName:  []byte("banana"),
			expectedOk: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, ok := test.input.Get(test.fieldName)
			if test.expectedOk {
				require.True(t, ok)
				require.Equal(t, test.expectedVal, val)
				return
			}
			require.False(t, ok)
		})
	}
}

func TestDocumentCompare(t *testing.T) {
	tests := []struct {
		name     string
		l, r     Document
		expected int
	}{
		{
			name:     "empty documents are equal",
			l:        Document{},
			r:        Document{},
			expected: 0,
		},
		{
			name: "documents with the same id and the same fields in the same order are equal",
			l: Document{
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
			r: Document{
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
			expected: 0,
		},
		{
			name: "documents are ordered by their IDs",
			l: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			r: Document{
				ID: []byte("831991"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			expected: 1,
		},
		{
			name: "documents are ordered by their field names",
			l: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
			r: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			expected: 1,
		},
		{
			name: "documents are ordered by their field values",
			l: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("green"),
					},
				},
			},
			r: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			expected: -1,
		},
		{
			name: "documents are ordered by their lengths",
			l: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			r: Document{
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
			expected: -1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.l.Compare(test.r))
		})
	}
}
func TestDocumentEquality(t *testing.T) {
	tests := []struct {
		name     string
		l, r     Document
		expected bool
	}{
		{
			name:     "empty documents are equal",
			l:        Document{},
			r:        Document{},
			expected: true,
		},
		{
			name: "documents with the same fields in the same order are equal",
			l: Document{
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
			r: Document{
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
			name: "documents with the same fields in different order are equal",
			l: Document{
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
			r: Document{
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
			name: "documents with different fields are unequal",
			l: Document{
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
			r: Document{
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
			l: Document{
				ID: []byte("831992"),
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			r: Document{
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
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.l.Equal(test.r))
		})
	}
}

func TestDocumentValidation(t *testing.T) {
	tests := []struct {
		name        string
		input       Document
		expectedErr bool
	}{
		{
			name:        "empty document",
			input:       Document{},
			expectedErr: true,
		},
		{
			name: "empty document w/ ID",
			input: Document{
				ID: []byte("foobar"),
			},
			expectedErr: false,
		},
		{
			name: "invalid UTF-8 in field name",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("\xff"),
						Value: []byte("bar"),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "invalid UTF-8 in field value",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("\xff"),
						Value: []byte("bar"),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "document contains field with reserved field name",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					Field{
						Name:  IDReservedFieldName,
						Value: []byte("123"),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "valid document",
			input: Document{
				Fields: []Field{
					Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
			expectedErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.input.Validate()
			if test.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDocumentHasID(t *testing.T) {
	tests := []struct {
		name     string
		input    Document
		expected bool
	}{
		{
			name: "nil ID",
			input: Document{
				ID: nil,
			},
			expected: false,
		},
		{
			name: "zero-length ID",
			input: Document{
				ID: make([]byte, 0, 16),
			},
			expected: false,
		},
		{
			name: "valid ID",
			input: Document{
				ID: []byte("831992"),
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.input.HasID())
		})
	}
}

func TestSortingDocuments(t *testing.T) {
	tests := []struct {
		name            string
		input, expected Documents
	}{
		{
			name: "unordered documents",
			input: Documents{
				Document{
					ID: []byte("831992"),
					Fields: []Field{
						Field{
							Name:  []byte("banana"),
							Value: []byte("yellow"),
						},
					},
				},
				Document{
					ID: []byte("831992"),
					Fields: []Field{
						Field{
							Name:  []byte("apple"),
							Value: []byte("red"),
						},
					},
				},
			},
			expected: Documents{
				Document{
					ID: []byte("831992"),
					Fields: []Field{
						Field{
							Name:  []byte("apple"),
							Value: []byte("red"),
						},
					},
				},
				Document{
					ID: []byte("831992"),
					Fields: []Field{
						Field{
							Name:  []byte("banana"),
							Value: []byte("yellow"),
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input
			sort.Sort(actual)
			require.Equal(t, len(test.expected), len(actual))
			fmt.Println(actual)
			for i := range test.expected {
				require.True(t, test.expected[i].Equal(actual[i]))
			}
		})
	}
}
