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

package builder

import (
	"fmt"
	"sort"
	"testing"
	"unsafe"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

var (
	testOptions = NewOptions()

	testDocuments = []doc.Metadata{
		{
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
		{
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
		{
			ID: []byte("42"),
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("pineapple"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
	}
)

func TestBuilderFields(t *testing.T) {
	builder, err := NewBuilderFromDocuments(testOptions)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, builder.Close())
	}()

	for i := 0; i < 10; i++ {
		builder.Reset()

		knownsFields := map[string]struct{}{}
		for _, d := range testDocuments {
			for _, f := range d.Fields {
				knownsFields[string(f.Name)] = struct{}{}
			}
			_, err = builder.Insert(d)
			require.NoError(t, err)
		}

		fieldsIter, err := builder.FieldsPostingsList()
		require.NoError(t, err)

		fields := toSlice(t, fieldsIter)
		for _, f := range fields {
			delete(knownsFields, string(f))
		}
		require.Empty(t, knownsFields)
	}
}

func TestBuilderTerms(t *testing.T) {
	builder, err := NewBuilderFromDocuments(testOptions)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, builder.Close())
	}()

	for i := 0; i < 10; i++ {
		builder.Reset()

		knownsFields := map[string]map[string]struct{}{}
		for _, d := range testDocuments {
			for _, f := range d.Fields {
				knownVals, ok := knownsFields[string(f.Name)]
				if !ok {
					knownVals = make(map[string]struct{})
					knownsFields[string(f.Name)] = knownVals
				}
				knownVals[string(f.Value)] = struct{}{}
			}
			_, err = builder.Insert(d)
			require.NoError(t, err)
		}

		for field, expectedTerms := range knownsFields {
			termsIter, err := builder.Terms([]byte(field))
			require.NoError(t, err)
			terms := toTermPostings(t, termsIter)
			for term := range terms {
				delete(expectedTerms, term)
			}
			require.Empty(t, expectedTerms)
		}
	}
}

// Test that calling Insert(...) API returns correct concrete errors
// instead of partial batch error type.
func TestBuilderInsertDuplicateReturnsErrDuplicateID(t *testing.T) {
	builder, err := NewBuilderFromDocuments(testOptions)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, builder.Close())
	}()

	_, err = builder.Insert(testDocuments[2])
	require.NoError(t, err)

	_, err = builder.Insert(testDocuments[2])
	require.Error(t, err)
	require.Equal(t, index.ErrDuplicateID, err)
}

func toSlice(t *testing.T, iter segment.FieldsPostingsListIterator) [][]byte {
	elems := [][]byte{}
	for iter.Next() {
		b, _ := iter.Current()
		elems = append(elems, b)
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return elems
}

type termPostings map[string][]int

func toTermPostings(t *testing.T, iter segment.TermsIterator) termPostings {
	elems := make(termPostings)
	for iter.Next() {
		term, postings := iter.Current()
		_, exists := elems[string(term)]
		require.False(t, exists)

		values := []int{}
		it := postings.Iterator()
		for it.Next() {
			values = append(values, int(it.Current()))
		}
		sort.Sort(sort.IntSlice(values))

		require.NoError(t, it.Err())
		require.NoError(t, it.Close())

		elems[string(term)] = values
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return elems
}

// nolint: unused
func printBuilder(t *testing.T, b segment.Builder) {
	fmt.Printf("print builder %x\n", unsafe.Pointer(b.(*builder)))
	fieldsIter, err := b.FieldsPostingsList()
	require.NoError(t, err)
	for fieldsIter.Next() {
		curr, _ := fieldsIter.Current()
		fmt.Printf("builder field: %v\n", string(curr))
		termsIter, err := b.Terms(curr)
		require.NoError(t, err)
		for termsIter.Next() {
			term, postings := termsIter.Current()
			postingsIter := postings.Iterator()
			for postingsIter.Next() {
				posting := postingsIter.Current()
				fmt.Printf("builder term: %s, doc: %d\n", string(term), posting)
			}
		}
	}
	fmt.Println()
}
