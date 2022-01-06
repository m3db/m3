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
	"fmt"
	"io"
	"sort"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	xerrors "github.com/m3db/m3/src/x/errors"
)

type builderFromSegments struct {
	docs           []doc.Metadata
	idSet          *IDsMap
	filter         segment.DocumentsFilter
	segments       []segmentMetadata
	termsIter      *termsIterFromSegments
	segmentsOffset postings.ID
}

type segmentMetadata struct {
	segment segment.Segment
	offset  postings.ID
	// negativeOffsets is a lookup of document IDs are duplicates or should be skipped,
	// that is documents that are already contained by other segments or should
	// not be included in the output segment and hence should not be returned
	// when looking up documents. If this is the case offset is -1.
	// If a document ID is not a duplicate or skipped then the offset is
	// the shift that should be applied when translating this postings ID
	// to the result postings ID.
	negativeOffsets []int64
	skips           int64
}

// NewBuilderFromSegments returns a new builder from segments.
func NewBuilderFromSegments(opts Options) segment.SegmentsBuilder {
	return &builderFromSegments{
		idSet: NewIDsMap(IDsMapOptions{
			InitialSize: opts.InitialCapacity(),
		}),
		termsIter: newTermsIterFromSegments(),
	}
}

func (b *builderFromSegments) Reset() {
	// Reset the documents slice
	var emptyDoc doc.Metadata
	for i := range b.docs {
		b.docs[i] = emptyDoc
	}
	b.docs = b.docs[:0]

	// Reset all entries in ID set
	b.idSet.Reset()

	// Reset the segments metadata
	b.segmentsOffset = 0
	var emptySegment segmentMetadata
	for i := range b.segments {
		// Save the offsets array.
		negativeOffsets := b.segments[i].negativeOffsets
		b.segments[i] = emptySegment
		b.segments[i].negativeOffsets = negativeOffsets[:0]
	}
	b.segments = b.segments[:0]

	b.termsIter.clear()
}

func (b *builderFromSegments) SetFilter(
	filter segment.DocumentsFilter,
) {
	b.filter = filter
}

func (b *builderFromSegments) AddSegments(segments []segment.Segment) error {
	// Order by largest -> smallest so that the first segment
	// is the largest when iterating over term postings lists
	// (which means it can be directly copied into the merged postings
	// list via a union rather than needing to shift posting list
	// IDs to take into account for duplicates).
	// Note: This must be done first so that offset is correctly zero
	// for the largest segment.
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Size() > segments[j].Size()
	})

	// numMaxDocs can sometimes be larger than the actual number of documents
	// since some are duplicates
	numMaxDocs := 0
	for _, segment := range segments {
		numMaxDocs += int(segment.Size())
	}

	// Ensure we don't have to constantly reallocate docs slice
	totalMaxSize := len(b.docs) + numMaxDocs
	if cap(b.docs) < totalMaxSize {
		b.docs = make([]doc.Metadata, 0, totalMaxSize)
	}

	// First build metadata and docs slice
	for _, segment := range segments {
		iter, closer, err := allDocsIter(segment)
		if err != nil {
			return err
		}

		var negativeOffsets []int64
		if n := len(b.segments); cap(b.segments) > n {
			// Take the offsets from the element we're about to reuse.
			negativeOffsets = b.segments[:n+1][n].negativeOffsets[:0]
		}
		if int64(cap(negativeOffsets)) < segment.Size() {
			negativeOffsets = make([]int64, 0, int(1.5*float64(segment.Size())))
		}

		var (
			added      int
			currOffset int64
		)
		for iter.Next() {
			d := iter.Current()
			negativeOffsets = append(negativeOffsets, currOffset)
			if b.idSet.Contains(d.ID) {
				// Skip duplicates.
				negativeOffsets[len(negativeOffsets)-1] = -1
				currOffset++
				if b.filter != nil {
					// Callback for when duplicate doc encountered and we filter
					// out the document from the resulting segment.
					b.filter.OnDuplicateDoc(d)
				}
				continue
			}
			if b.filter != nil && !b.filter.ContainsDoc(d) {
				// Actively filtering and ID is not contained.
				negativeOffsets[len(negativeOffsets)-1] = -1
				currOffset++
				continue
			}
			b.idSet.SetUnsafe(d.ID, struct{}{}, IDsMapSetUnsafeOptions{
				NoCopyKey:     true,
				NoFinalizeKey: true,
			})
			b.docs = append(b.docs, d)
			added++
		}

		err = xerrors.FirstError(iter.Err(), iter.Close(), closer.Close())
		if err != nil {
			return err
		}

		b.segments = append(b.segments, segmentMetadata{
			segment:         segment,
			offset:          b.segmentsOffset,
			negativeOffsets: negativeOffsets,
			skips:           currOffset,
		})
		b.segmentsOffset += postings.ID(added)
	}

	// Make sure the terms iter has all the segments to combine data from
	b.termsIter.reset(b.segments)

	return nil
}

func (b *builderFromSegments) SegmentMetadatas() ([]segment.SegmentsBuilderSegmentMetadata, error) {
	n := len(b.segments)
	if n < 1 {
		return nil, fmt.Errorf("segments empty: length=%d", n)
	}

	result := make([]segment.SegmentsBuilderSegmentMetadata, 0, n)
	for _, s := range b.segments {
		result = append(result, segment.SegmentsBuilderSegmentMetadata{
			Segment:         s.segment,
			Offset:          s.offset,
			NegativeOffsets: s.negativeOffsets,
			Skips:           s.skips,
		})
	}

	return result, nil
}

func (b *builderFromSegments) Docs() []doc.Metadata {
	return b.docs
}

func (b *builderFromSegments) AllDocs() (index.IDDocIterator, error) {
	rangeIter := postings.NewRangeIterator(0, postings.ID(len(b.docs)))
	return index.NewIDDocIterator(b, rangeIter), nil
}

func (b *builderFromSegments) Metadata(id postings.ID) (doc.Metadata, error) {
	idx := int(id)
	if idx < 0 || idx >= len(b.docs) {
		return doc.Metadata{}, errDocNotFound
	}

	return b.docs[idx], nil
}

func (b *builderFromSegments) NumDocs() (int, error) {
	return len(b.docs), nil
}

func (b *builderFromSegments) FieldsIterable() segment.FieldsIterable {
	return b
}

func (b *builderFromSegments) TermsIterable() segment.TermsIterable {
	return b
}

func (b *builderFromSegments) Fields() (segment.FieldsIterator, error) {
	return newFieldIterFromSegments(b.segments)
}

func (b *builderFromSegments) FieldsPostingsList() (segment.FieldsPostingsListIterator, error) {
	return newFieldPostingsListIterFromSegments(b.segments)
}

func (b *builderFromSegments) Terms(field []byte) (segment.TermsIterator, error) {
	if err := b.termsIter.setField(field); err != nil {
		return nil, err
	}
	return b.termsIter, nil
}

func allDocsIter(seg segment.Segment) (index.IDDocIterator, io.Closer, error) {
	reader, err := seg.Reader()
	if err != nil {
		return nil, nil, err
	}

	iter, err := reader.AllDocs()
	if err != nil {
		return nil, nil, err
	}

	return iter, reader, nil
}
