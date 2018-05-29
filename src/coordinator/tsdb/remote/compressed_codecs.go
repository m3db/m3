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

package remote

import (
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/src/coordinator/generated/proto/rpc"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3db/src/dbnode/x/xpool"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

func initializeVars() {
	opts = checked.NewBytesOptions().SetFinalizer(
		checked.BytesFinalizerFn(func(b checked.Bytes) {
			b.Reset(nil)
		}))

	iterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
}

var (
	opts       checked.BytesOptions
	iterAlloc  func(r io.Reader) encoding.ReaderIterator
	initialize sync.Once
)

func compressedSegmentFromBlockReader(br xio.BlockReader) (*rpc.Segment, error) {
	segment, err := br.Segment()
	if err != nil {
		return nil, err
	}
	return &rpc.Segment{
		Head:      segment.Head.Bytes(),
		Tail:      segment.Tail.Bytes(),
		StartTime: xtime.ToNanoseconds(br.Start),
		BlockSize: int64(br.BlockSize),
	}, nil
}

func compressedSegmentsFromReaders(readers xio.ReaderSliceOfSlicesIterator) (*rpc.Segments, error) {
	segments := &rpc.Segments{}
	l, _, _ := readers.CurrentReaders()
	// NB(arnikola) If there's only a single reader, the segment has been merged
	// otherwise, multiple unmerged segments exist.
	if l == 1 {
		br := readers.CurrentReaderAt(0)
		segment, err := compressedSegmentFromBlockReader(br)
		if err != nil {
			return nil, err
		}
		segments.Merged = segment
	} else {
		unmerged := make([]*rpc.Segment, 0, l)
		for i := 0; i < l; i++ {
			br := readers.CurrentReaderAt(i)
			segment, err := compressedSegmentFromBlockReader(br)
			if err != nil {
				return nil, err
			}
			unmerged = append(unmerged, segment)
		}
		segments.Unmerged = unmerged
	}
	return segments, nil
}

func compressedTagsFromTagIterator(tagIter ident.TagIterator) ([]*rpc.CompressedTag, error) {
	tags := make([]*rpc.CompressedTag, 0, tagIter.Remaining())
	for tagIter.Next() {
		tag := tagIter.Current()
		tags = append(tags, &rpc.CompressedTag{
			Name:  tag.Name.Bytes(),
			Value: tag.Value.Bytes(),
		})
	}
	err := tagIter.Err()
	if err != nil {
		return nil, err
	}
	return tags, nil
}

// CompressedProtobufFromSeriesIterator builds compressed rpc series from a SeriesIterator
func CompressedProtobufFromSeriesIterator(it encoding.SeriesIterator) (*rpc.Series, error) {
	initialize.Do(initializeVars)

	replicas := it.Replicas()
	compressedReplicas := make([]*rpc.CompressedValuesReplica, 0, len(replicas))
	for _, replica := range replicas {
		replicaSegments := make([]*rpc.Segments, 0, len(replicas))
		readers := replica.Readers()
		for next := true; next; next = readers.Next() {
			segments, err := compressedSegmentsFromReaders(readers)
			if err != nil {
				return nil, err
			}
			replicaSegments = append(replicaSegments, segments)
		}
		compressedReplicas = append(compressedReplicas, &rpc.CompressedValuesReplica{
			Segments: replicaSegments,
		})
	}

	tags, err := compressedTagsFromTagIterator(it.Tags())
	if err != nil {
		return nil, err
	}

	start := xtime.ToNanoseconds(it.Start())
	end := xtime.ToNanoseconds(it.End())

	compressedDatapoints := &rpc.CompressedDatapoints{
		Namespace: it.Namespace().Bytes(),
		StartTime: start,
		EndTime:   end,
		Replicas:  compressedReplicas,
		Tags:      tags,
	}

	return &rpc.Series{
		Id:         it.ID().Bytes(),
		Compressed: compressedDatapoints,
	}, nil
}

func segmentBytesFromCompressedSegment(
	segHead, segTail []byte,
	opts checked.BytesOptions,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) (checked.Bytes, checked.Bytes) {
	var head, tail checked.Bytes
	if checkedBytesWrapperPool != nil {
		head = checkedBytesWrapperPool.Get(segHead)
		tail = checkedBytesWrapperPool.Get(segTail)
	} else {
		head = checked.NewBytes(segHead, opts)
		tail = checked.NewBytes(segTail, opts)
	}
	return head, tail
}

func blockReaderFromCompressedSegment(
	seg *rpc.Segment,
	opts checked.BytesOptions,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) xio.BlockReader {
	head, tail := segmentBytesFromCompressedSegment(seg.GetHead(), seg.GetTail(), opts, checkedBytesWrapperPool)
	segment := ts.NewSegment(head, tail, ts.FinalizeNone)
	segmentReader := xio.NewSegmentReader(segment)

	return xio.BlockReader{
		SegmentReader: segmentReader,
		Start:         xtime.FromNanoseconds(seg.GetStartTime()),
		BlockSize:     time.Duration(seg.GetBlockSize()),
	}
}

func tagIteratorFromCompressedTags(compressedTags []*rpc.CompressedTag, idPool ident.Pool) ident.TagIterator {
	var (
		tags    ident.Tags
		tagIter ident.TagsIterator
	)
	if idPool != nil {
		tags = idPool.Tags()
	} else {
		tags = ident.NewTags()
	}

	for _, tag := range compressedTags {
		name, value := string(tag.GetName()), string(tag.GetValue())
		if idPool != nil {
			tags.Append(idPool.StringTag(name, value))
		} else {
			tags.Append(ident.StringTag(name, value))
		}
	}

	if idPool != nil {
		tagIter = idPool.TagsIterator()
		tagIter.Reset(tags)
	} else {
		tagIter = ident.NewTagsIterator(tags)
	}

	return tagIter
}

func blockReadersFromCompressedSegments(
	segments []*rpc.Segments,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) [][]xio.BlockReader {
	blockReaders := make([][]xio.BlockReader, len(segments))

	for i, segment := range segments {
		blockReadersPerSegment := make([]xio.BlockReader, 0, len(segments))
		mergedSegment := segment.GetMerged()
		if mergedSegment != nil {
			reader := blockReaderFromCompressedSegment(mergedSegment, opts, checkedBytesWrapperPool)
			blockReadersPerSegment = append(blockReadersPerSegment, reader)
		} else {
			unmerged := segment.GetUnmerged()
			for _, seg := range unmerged {
				reader := blockReaderFromCompressedSegment(seg, opts, checkedBytesWrapperPool)
				blockReadersPerSegment = append(blockReadersPerSegment, reader)
			}
		}
		blockReaders[i] = blockReadersPerSegment
	}

	return blockReaders
}

// SeriesIteratorFromCompressedProtobuf creates a SeriesIterator from a compressed protobuf
func SeriesIteratorFromCompressedProtobuf(iteratorPools encoding.IteratorPools, timeSeries *rpc.Series) encoding.SeriesIterator {
	initialize.Do(initializeVars)

	var (
		multiReaderPool encoding.MultiReaderIteratorPool
		seriesPool      encoding.SeriesIteratorPool

		checkedBytesWrapperPool xpool.CheckedBytesWrapperPool
		idPool                  ident.Pool

		allReplicaIterators []encoding.MultiReaderIterator
	)

	compressedValues := timeSeries.GetCompressed()
	replicas := compressedValues.GetReplicas()

	// Set up iterator pools if available
	if iteratorPools != nil {
		multiReaderPool = iteratorPools.MultiReaderIterator()
		seriesPool = iteratorPools.SeriesIterator()
		checkedBytesWrapperPool = iteratorPools.CheckedBytesWrapper()
		idPool = iteratorPools.ID()

		allReplicaIterators = iteratorPools.MultiReaderIteratorArray().Get(len(replicas))
	} else {
		allReplicaIterators = make([]encoding.MultiReaderIterator, 0, len(replicas))
	}

	for _, replica := range replicas {
		blockReaders := blockReadersFromCompressedSegments(replica.GetSegments(), checkedBytesWrapperPool)

		// TODO arnikola investigate pooling these?
		sliceOfSlicesIterator := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(blockReaders)
		perReplicaIterator := encoding.NewMultiReaderIterator(iterAlloc, multiReaderPool)
		perReplicaIterator.ResetSliceOfSlices(sliceOfSlicesIterator)

		allReplicaIterators = append(allReplicaIterators, perReplicaIterator)
	}

	var (
		idString, nsString = string(timeSeries.GetId()), string(compressedValues.GetNamespace())
		id, ns             ident.ID
	)
	if idPool != nil {
		id = idPool.StringID(idString)
		ns = idPool.StringID(nsString)
	} else {
		id = ident.StringID(idString)
		ns = ident.StringID(nsString)
	}

	tagIter := tagIteratorFromCompressedTags(compressedValues.GetTags(), idPool)
	start := xtime.FromNanoseconds(compressedValues.GetStartTime())
	end := xtime.FromNanoseconds(compressedValues.GetEndTime())
	return encoding.NewSeriesIterator(id, ns, tagIter, start, end, allReplicaIterators, seriesPool)
}
