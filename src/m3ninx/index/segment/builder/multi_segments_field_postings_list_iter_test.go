// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

func TestFieldPostingsListIterFromSegments(t *testing.T) {
	segments := []segment.Segment{
		// nolint: dupl
		newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("bux_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val0")},
				},
			},
			{
				ID: []byte("bar_0"),
				Fields: []doc.Field{
					{Name: []byte("cat"), Value: []byte("rhymes")},
					{Name: []byte("hat"), Value: []byte("with")},
					{Name: []byte("bat"), Value: []byte("pat")},
				},
			},
		}),
		// nolint: dupl
		newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("foo_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val0")},
				},
			},
			{
				ID: []byte("bux_1"),
				Fields: []doc.Field{
					{Name: []byte("delta"), Value: []byte("22")},
					{Name: []byte("gamma"), Value: []byte("33")},
					{Name: []byte("theta"), Value: []byte("44")},
				},
			},
		}),
		newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("bar_1"),
				Fields: []doc.Field{
					{Name: []byte("cat"), Value: []byte("rhymes")},
					{Name: []byte("hat"), Value: []byte("with")},
					{Name: []byte("bat"), Value: []byte("pat")},
				},
			},
			{
				ID: []byte("foo_1"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val1")},
				},
			},
			{
				ID: []byte("baz_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("watermelon")},
					{Name: []byte("color"), Value: []byte("green")},
					{Name: []byte("alpha"), Value: []byte("0.5")},
				},
			},
			{
				ID: []byte("bux_2"),
				Fields: []doc.Field{
					{Name: []byte("delta"), Value: []byte("22")},
					{Name: []byte("gamma"), Value: []byte("33")},
					{Name: []byte("theta"), Value: []byte("44")},
				},
			},
		}),
	}
	builder := NewBuilderFromSegments(testOptions)
	builder.Reset()

	b, ok := builder.(*builderFromSegments)
	require.True(t, ok)
	require.NoError(t, builder.AddSegments(segments))
	iter, err := b.FieldsPostingsList()
	require.NoError(t, err)
	// Perform both present/not present checks per field/field postings list.
	for iter.Next() {
		field, pl := iter.Current()
		docIter, err := b.AllDocs()
		require.NoError(t, err)
		for docIter.Next() {
			doc := docIter.Current()
			pID := docIter.PostingsID()
			found := checkIfFieldExistsInDoc(field, doc)
			require.Equal(t, found, pl.Contains(pID))
		}
	}
}

func checkIfFieldExistsInDoc(
	field []byte,
	doc doc.Metadata,
) bool {
	found := false
	for _, f := range doc.Fields {
		if bytes.Equal(field, f.Name) {
			found = true
		}
	}
	return found
}
