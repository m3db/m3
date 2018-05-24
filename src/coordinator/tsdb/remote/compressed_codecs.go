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

func blockReaderToSegment(br xio.BlockReader) (*rpc.Segment, error) {
	segment, err := br.Segment()
	if err != nil {
		return nil, err
	}
	return &rpc.Segment{
		Head: segment.Head.Bytes(),
		Tail: segment.Tail.Bytes(),
	}, nil
}

// RPCFromSeriesIterator converts a seriesIterator to a grpc series
func RPCFromSeriesIterator(it encoding.SeriesIterator) (*rpc.Series, error) {
	defer it.Close()

	replicas := it.Replicas()
	compressedReplicas := make([]*rpc.CompressedValuesReplica, 0, len(replicas))
	for _, replica := range replicas {
		replicaSegments := make([]*rpc.Segments, 0, len(replicas))
		readers := replica.Readers()
		for next := true; next; next = readers.Next() {
			segments := &rpc.Segments{}
			l, _, _ := readers.CurrentReaders()
			// NB(arnikola) If there's only a single reader, the segment has been merged
			// otherwise, multiple unmerged segments exist.
			if l == 1 {
				br := readers.CurrentReaderAt(0)
				segment, err := blockReaderToSegment(br)
				if err != nil {
					return nil, err
				}
				segments.Merged = segment
			} else {
				unmerged := make([]*rpc.Segment, 0, l)
				for i := 0; i < l; i++ {
					br := readers.CurrentReaderAt(i)
					segment, err := blockReaderToSegment(br)
					if err != nil {
						return nil, err
					}
					unmerged = append(unmerged, segment)
				}
				segments.Unmerged = unmerged
			}
			replicaSegments = append(replicaSegments, segments)
		}
		compressedReplicas = append(compressedReplicas, &rpc.CompressedValuesReplica{
			Segments: replicaSegments,
		})
	}

	start := xtime.ToNanoseconds(it.Start())
	end := xtime.ToNanoseconds(it.End())

	tagIter := it.Tags()
	tags := make([]*rpc.CompressedTag, 0, tagIter.Remaining())
	for tagIter.Next() {
		tag := tagIter.Current()
		tags = append(tags, &rpc.CompressedTag{
			Name:  tag.Name.String(),
			Value: tag.Value.String(),
		})
	}

	compressedDatapoints := &rpc.CompressedDatapoints{
		Namespace: it.Namespace().String(),
		StartTime: start,
		EndTime:   end,
		Replicas:  compressedReplicas,
		Tags:      tags,
	}

	return &rpc.Series{
		Id:         it.ID().String(),
		Compressed: compressedDatapoints,
	}, nil
}

func segmentToBlockReader(
	seg *rpc.Segment,
	opts checked.BytesOptions,
	iteratorPools encoding.IteratorPools,
) xio.BlockReader {
	var checkedBytesWrapperPool xpool.CheckedBytesWrapperPool
	if iteratorPools != nil {
		checkedBytesWrapperPool = iteratorPools.CheckedBytesWrapper()
	}

	var head, tail checked.Bytes
	if checkedBytesWrapperPool != nil {
		head = checkedBytesWrapperPool.Get(seg.Head)
		tail = checkedBytesWrapperPool.Get(seg.Tail)
	} else {
		head = checked.NewBytes(seg.Head, opts)
		tail = checked.NewBytes(seg.Tail, opts)
	}

	segment := ts.NewSegment(head, tail, ts.FinalizeNone)
	segmentReader := xio.NewSegmentReader(segment)

	return xio.BlockReader{
		SegmentReader: segmentReader,
		Start:         time.Time{},
		BlockSize:     0,
	}
}

var (
	opts = checked.NewBytesOptions().SetFinalizer(
		checked.BytesFinalizerFn(func(b checked.Bytes) {
			b.Reset(nil)
		}))

	iterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
)

// SeriesIteratorFromRPC converts a grpc series to a seriesIterator
func SeriesIteratorFromRPC(iteratorPools encoding.IteratorPools, timeSeries *rpc.Series) encoding.SeriesIterator {
	var (
		mutliReaderArrayPool encoding.MultiReaderIteratorArrayPool
		multiReaderPool      encoding.MultiReaderIteratorPool
		seriesPool           encoding.SeriesIteratorPool

		idPool ident.Pool

		allReplicaIterators []encoding.MultiReaderIterator
	)

	if iteratorPools != nil {
		multiReaderPool = iteratorPools.MultiReaderIterator()
		seriesPool = iteratorPools.SeriesIterator()
		mutliReaderArrayPool = iteratorPools.MultiReaderIteratorArray()
		idPool = iteratorPools.ID()
	}

	compressedValues := timeSeries.GetCompressed()
	replicas := compressedValues.GetReplicas()

	if mutliReaderArrayPool != nil {
		allReplicaIterators = mutliReaderArrayPool.Get(len(replicas))
	} else {
		allReplicaIterators = make([]encoding.MultiReaderIterator, 0, len(replicas))
	}

	for _, replica := range replicas {
		segments := replica.GetSegments()
		blockReaders := make([][]xio.BlockReader, len(segments))

		for i, segment := range segments {
			blockReadersPerSegment := make([]xio.BlockReader, 0, len(segments))
			mergedSegment := segment.GetMerged()
			if mergedSegment != nil {
				reader := segmentToBlockReader(mergedSegment, opts, iteratorPools)
				blockReadersPerSegment = append(blockReadersPerSegment, reader)
			} else {
				unmerged := segment.GetUnmerged()
				for _, seg := range unmerged {
					reader := segmentToBlockReader(seg, opts, iteratorPools)
					blockReadersPerSegment = append(blockReadersPerSegment, reader)
				}
			}
			blockReaders[i] = blockReadersPerSegment
		}

		// TODO arnikola investigate pooling these?
		sliceOfSlicesIterator := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(blockReaders)
		perReplicaIterator := encoding.NewMultiReaderIterator(iterAlloc, multiReaderPool)
		perReplicaIterator.ResetSliceOfSlices(sliceOfSlicesIterator)

		allReplicaIterators = append(allReplicaIterators, perReplicaIterator)
	}

	var (
		id, ns  ident.ID
		tags    ident.Tags
		tagIter ident.TagsIterator
	)

	if idPool != nil {
		id = idPool.StringID(timeSeries.GetId())
		ns = idPool.StringID(compressedValues.GetNamespace())
		tags = idPool.Tags()
	} else {
		id = ident.StringID(timeSeries.GetId())
		ns = ident.StringID(compressedValues.GetNamespace())
		tags = ident.NewTags()
	}

	for _, tag := range compressedValues.GetTags() {
		name, value := tag.GetName(), tag.GetValue()
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

	start := xtime.FromNanoseconds(compressedValues.GetStartTime())
	end := xtime.FromNanoseconds(compressedValues.GetEndTime())
	return encoding.NewSeriesIterator(id, ns, tagIter, start, end, allReplicaIterators, seriesPool)
}
