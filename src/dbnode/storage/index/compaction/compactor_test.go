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
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"

	"github.com/m3db/m3x/pool"
	"github.com/stretchr/testify/require"
)

var (
	testFSTSegmentOptions = fst.NewOptions()
	testMemSegmentOptions = mem.NewOptions()

	testDocuments = []doc.Document{
		doc.Document{
			ID: []byte("one"),
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
		doc.Document{
			ID: []byte("two"),
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
	}

	testDocsMaxBatch = 8
	testDocsPool     = doc.NewDocumentArrayPool(doc.DocumentArrayPoolOpts{
		Options:  pool.NewObjectPoolOptions().SetSize(1),
		Capacity: testDocsMaxBatch,
	})
)

func init() {
	testDocsPool.Init()
}

func TestCompactorSingleMutableSegment(t *testing.T) {
	seg, err := mem.NewSegment(0, testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[0])
	require.NoError(t, err)

	_, err = seg.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testDocsPool, testDocsMaxBatch,
		testMemSegmentOptions, testFSTSegmentOptions)
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{seg})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func TestCompactorManySegments(t *testing.T) {
	seg1, err := mem.NewSegment(0, testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg1.Insert(testDocuments[0])
	require.NoError(t, err)

	seg2, err := mem.NewSegment(0, testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testDocsPool, testDocsMaxBatch,
		testMemSegmentOptions, testFSTSegmentOptions)
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{seg1, seg2})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func TestCompactorCompactDuplicateIDsNoError(t *testing.T) {
	seg1, err := mem.NewSegment(0, testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg1.Insert(testDocuments[0])
	require.NoError(t, err)

	seg2, err := mem.NewSegment(0, testMemSegmentOptions)
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[0])
	require.NoError(t, err)

	_, err = seg2.Insert(testDocuments[1])
	require.NoError(t, err)

	compactor, err := NewCompactor(testDocsPool, testDocsMaxBatch,
		testMemSegmentOptions, testFSTSegmentOptions)
	require.NoError(t, err)

	compacted, err := compactor.Compact([]segment.Segment{seg1, seg2})
	require.NoError(t, err)

	assertContents(t, compacted, testDocuments)

	require.NoError(t, compactor.Close())
}

func assertContents(t *testing.T, seg segment.Segment, docs []doc.Document) {
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
