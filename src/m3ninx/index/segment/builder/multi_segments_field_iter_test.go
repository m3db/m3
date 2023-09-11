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

package builder

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/stretchr/testify/require"
)

var (
	testMemOptions = mem.NewOptions()
)

func TestFieldIterFromSegmentsDeduplicates(t *testing.T) {
	segments := []segmentMetadata{
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("foo"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
		})},
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("bar"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("banana")},
					{Name: []byte("meat"), Value: []byte("beef")},
				},
			},
			{
				ID: []byte("baz"),
				Fields: []doc.Field{
					{Name: []byte("color"), Value: []byte("blue")},
					{Name: []byte("alpha"), Value: []byte("1.0")},
				},
			},
		})},
	}

	iter, err := newFieldIterFromSegments(segments)
	require.NoError(t, err)

	assertIterValues(t, iter, []string{
		string(doc.IDReservedFieldName),
		"alpha",
		"color",
		"fruit",
		"meat",
		"vegetable",
	})
}

func TestFieldIterFromSegmentsSomeEmpty(t *testing.T) {
	segments := []segmentMetadata{
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("foo"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
		})},
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{})},
	}

	iter, err := newFieldIterFromSegments(segments)
	require.NoError(t, err)

	assertIterValues(t, iter, []string{
		string(doc.IDReservedFieldName),
		"fruit",
		"vegetable",
	})
}

func TestFieldIterFromSegmentsIdentical(t *testing.T) {
	segments := []segmentMetadata{
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("foo"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
		})},
		{segment: newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("bar"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
		})},
	}

	iter, err := newFieldIterFromSegments(segments)
	require.NoError(t, err)

	assertIterValues(t, iter, []string{
		string(doc.IDReservedFieldName),
		"fruit",
		"vegetable",
	})
}

func assertIterValues(
	t *testing.T,
	iter segment.FieldsIterator,
	vals []string,
) {
	var actual []string
	for iter.Next() {
		actual = append(actual, string(iter.Current()))
	}
	require.False(t, iter.Next())

	require.Equal(t, vals, actual)
	require.NoError(t, xerrors.FirstError(iter.Err(), iter.Close()))
}

func newTestSegmentWithDocs(
	t *testing.T,
	docs []doc.Metadata,
) segment.Segment {
	seg, err := mem.NewSegment(testMemOptions)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, seg.Seal())
	}()

	if len(docs) == 0 {
		return seg
	}

	require.NoError(t, seg.InsertBatch(index.Batch{
		Docs: docs,
	}))

	return seg
}
