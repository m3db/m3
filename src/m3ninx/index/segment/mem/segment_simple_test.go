// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

func newTestOptions() Options {
	return NewOptions()
}

func TestNewMemSegment(t *testing.T) {
	opts := newTestOptions()
	idx, err := New(1, opts)
	require.NoError(t, err)

	testDoc := doc.Document{
		ID: []byte("some-random-id"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("one")},
			doc.Field{Name: []byte("def"), Value: doc.Value("two")},
		},
	}

	require.NoError(t, idx.Insert(testDoc))
	docsIter, err := idx.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("one"),
			},
		},
	})
	require.NoError(t, err)

	require.True(t, docsIter.Next())
	result, tombstoned := docsIter.Current()
	require.Equal(t, testDoc, result)
	require.False(t, tombstoned)
	require.False(t, docsIter.Next())
}
