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

package compaction

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

var (
	testFSTSegmentOptions     = fst.NewOptions()
	testMemSegmentOptions     = mem.NewOptions()
	testBuilderSegmentOptions = builder.NewOptions()

	testDocuments = []doc.Metadata{
		{
			ID: []byte("one"),
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
			ID: []byte("two"),
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
	}

	testMetadataMaxBatch = 8
	testMetadataPool     = doc.NewMetadataArrayPool(doc.MetadataArrayPoolOpts{
		Options:  pool.NewObjectPoolOptions().SetSize(1),
		Capacity: testMetadataMaxBatch,
	})
)

func init() {
	testMetadataPool.Init()
}

func TestCompactorSingleMutableSegment(t *testing.T) {
	seg, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[0])
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testMetadataPool, testMetadataMaxBatch,
		testBuilderSegmentOptions, testFSTSegmentOptions, CompactorOptions{})
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{
		mustSeal(t, seg),
	}, mmap.ReporterOptions{})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func TestCompactorSingleMutableSegmentWithMmapDocsData(t *testing.T) {
	seg, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[0])
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testMetadataPool, testMetadataMaxBatch,
		testBuilderSegmentOptions, testFSTSegmentOptions, CompactorOptions{
			MmapDocsData: true,
		})
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{
		mustSeal(t, seg),
	}, mmap.ReporterOptions{})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func TestCompactorManySegments(t *testing.T) {
	seg1, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg1.Insert(testDocuments[0])
	require.NoError(t, err)

	seg2, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testMetadataPool, testMetadataMaxBatch,
		testBuilderSegmentOptions, testFSTSegmentOptions, CompactorOptions{})
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{
		mustSeal(t, seg1),
		mustSeal(t, seg2),
	}, mmap.ReporterOptions{})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func TestCompactorCompactDuplicateIDsNoError(t *testing.T) {
	seg1, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg1.Insert(testDocuments[0])
	require.NoError(t, err)

	seg2, err := mem.NewSegment(testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[0])
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testMetadataPool, testMetadataMaxBatch,
		testBuilderSegmentOptions, testFSTSegmentOptions, CompactorOptions{})
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{
		mustSeal(t, seg1),
		mustSeal(t, seg2),
	}, mmap.ReporterOptions{})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func assertContents(t *testing.T, seg segment.Segment, docs []doc.Metadata) {
	// Ensure has contents
	require.Equal(t, int64(len(docs)), seg.Size())
	reader, err := seg.Reader()
	require.NoError(t, err)

	iter, err := reader.AllDocs()
	require.NoError(t, err)

	foundDocIDs := make(map[string]int)
	for iter.Next() {
		doc := iter.Current()

		found := false
		for _, testDoc := range docs {
			if string(doc.ID) == string(testDoc.ID) {
				found = true
				foundDocIDs[string(doc.ID)]++
				break
			}
		}
		require.True(t, found,
			fmt.Sprintf("doc_id=\"%s\" not found", string(doc.ID)))
	}
	require.Equal(t, len(docs), len(foundDocIDs))
	for _, count := range foundDocIDs {
		require.Equal(t, 1, count)
	}

	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func mustSeal(t *testing.T, seg segment.MutableSegment) segment.MutableSegment {
	require.NoError(t, seg.Seal())
	return seg
}
