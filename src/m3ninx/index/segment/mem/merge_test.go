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

package mem

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"

	"github.com/stretchr/testify/require"
)

func TestMemSegmentMerge(t *testing.T) {
	docs := []doc.Metadata{
		{
			ID: []byte("abc"),
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
			ID: []byte("cde"),
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
			ID: []byte("dfg"),
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
	d := docs[0]
	rest := docs[1:]

	opts := NewOptions()
	m1, err := NewSegment(opts)
	require.NoError(t, err)
	_, err = m1.Insert(d)
	require.NoError(t, err)

	m2, err := NewSegment(opts)
	require.NoError(t, err)
	for _, d := range rest {
		_, err = m2.Insert(d)
		require.NoError(t, err)
	}

	m3, err := NewSegment(opts)
	require.NoError(t, err)

	require.NoError(t, Merge(m3, m1, m2))

	reader, err := m3.Reader()
	require.NoError(t, err)

	for _, d := range docs {
		assertReaderHasDoc(t, reader, d)
	}

	require.NoError(t, reader.Close())
}

func assertReaderHasDoc(t *testing.T, r index.Reader, d doc.Metadata) {
	iter, err := r.AllDocs()
	require.NoError(t, err)
	found := false
	for iter.Next() {
		di := iter.Current()
		if di.Equal(d) {
			found = true
			break
		}
	}
	require.True(t, found)
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}
