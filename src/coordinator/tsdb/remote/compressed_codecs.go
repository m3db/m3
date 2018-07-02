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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/generated/proto/rpc"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/serialize"
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

func compressedTagsFromTagIteratorWithEncoder(tagIter ident.TagIterator, encoderPool serialize.TagEncoderPool) ([]byte, error) {
	encoder := encoderPool.Get()
	err := encoder.Encode(tagIter)
	if err != nil {
		return nil, err
	}
	defer encoder.Finalize()
	data, encoded := encoder.Data()
	if !encoded {
		return nil, fmt.Errorf("no refs available to data")
	}
	return data.Bytes(), nil
}

func tagsFromTagIterator(tagIter ident.TagIterator) ([]*rpc.Tag, error) {
	tags := make([]*rpc.Tag, 0, tagIter.Remaining())
	for tagIter.Next() {
		tag := tagIter.Current()
		tags = append(tags, &rpc.Tag{
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

func buildTags(tagIter ident.TagIterator, iterPools encoding.IteratorPools) ([]byte, []*rpc.Tag, error) {
	if iterPools != nil {
		encoderPool := iterPools.TagEncoder()
		if encoderPool != nil {
			compressedTags, err := compressedTagsFromTagIteratorWithEncoder(tagIter, encoderPool)
			return compressedTags, nil, err
		}
	}
	tags, err := tagsFromTagIterator(tagIter)
	return nil, tags, err
}

/*
Builds compressed rpc series from a SeriesIterator
SeriesIterator is the top level iterator returned by m3db
This SeriesIterator contains MultiReaderIterators, each representing a single replica
Each MultiReaderIterator has a ReaderSliceOfSlicesIterator where each step through the
iterator exposes a slice of underlying BlockReaders. Each BlockReader contains the
run time encoded bytes that represent the series.

SeriesIterator also has a TagIterator representing the tags associated with it

This function transforms a SeriesIterator into a protobuf representation to be able
to send it across the wire without needing to expand the series
*/
func compressedSeriesFromSeriesIterator(it encoding.SeriesIterator, iterPools encoding.IteratorPools) (*rpc.Series, error) {
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

	start := xtime.ToNanoseconds(it.Start())
	end := xtime.ToNanoseconds(it.End())

	compressedTags, tags, err := buildTags(it.Tags(), iterPools)
	if err != nil {
		return nil, err
	}

	compressedDatapoints := &rpc.CompressedDatapoints{
		Namespace:      it.Namespace().Bytes(),
		StartTime:      start,
		EndTime:        end,
		Replicas:       compressedReplicas,
		CompressedTags: compressedTags,
	}

	return &rpc.Series{
		Id:         it.ID().Bytes(),
		Compressed: compressedDatapoints,
		Tags:       tags,
	}, nil
}

// EncodeToCompressedFetchResult encodes SeriesIterators to compressed fetch results
func EncodeToCompressedFetchResult(
	iterators encoding.SeriesIterators,
	iterPools encoding.IteratorPools,
) (*rpc.FetchResult, error) {
	iters := iterators.Iters()
	seriesList := make([]*rpc.Series, 0, len(iters))
	for _, iter := range iters {
		series, err := compressedSeriesFromSeriesIterator(iter, iterPools)
		if err != nil {
			return nil, err
		}
		seriesList = append(seriesList, series)
	}

	return &rpc.FetchResult{
		Series: seriesList,
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

func tagIteratorFromCompressedTagsWithDecoder(
	compressedTags []byte,
	iterPools encoding.IteratorPools,
) (ident.TagIterator, error) {
	if iterPools == nil || iterPools.CheckedBytesWrapper() == nil || iterPools.TagDecoder() == nil {
		return nil, errors.ErrCannotDecodeCompressedTags
	}
	checkedBytes := iterPools.CheckedBytesWrapper().Get(compressedTags)
	decoder := iterPools.TagDecoder().Get()
	decoder.Reset(checkedBytes)
	defer decoder.Close()
	// Copy underlying TagIterator bytes before closing the decoder and returning it to the pool
	return decoder.Duplicate(), nil
}

func tagIteratorFromTags(compressedTags []*rpc.Tag, idPool ident.Pool) ident.TagIterator {
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

func tagIteratorFromSeries(series *rpc.Series, iteratorPools encoding.IteratorPools) (ident.TagIterator, error) {
	compressedValues := series.GetCompressed()
	if compressedValues != nil && len(compressedValues.GetCompressedTags()) > 0 {
		return tagIteratorFromCompressedTagsWithDecoder(compressedValues.GetCompressedTags(), iteratorPools)
	}
	var idPool ident.Pool
	if iteratorPools != nil {
		idPool = iteratorPools.ID()
	}
	return tagIteratorFromTags(series.GetTags(), idPool), nil
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

/*
Creates a SeriesIterator from a compressed protobuf. This is the reverse of
CompressedSeriesFromSeriesIterator, and takes an optional iteratorPool
argument that allows reuse of the underlying iterator pools from the m3db session
*/
func seriesIteratorFromCompressedSeries(
	timeSeries *rpc.Series,
	iteratorPools encoding.IteratorPools,
) (encoding.SeriesIterator, error) {
	initialize.Do(initializeVars)

	// Attempt to decompress compressed tags first as this is the only scenario that is expected to fail
	tagIter, err := tagIteratorFromSeries(timeSeries, iteratorPools)
	if err != nil {
		return nil, err
	}

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

	start := xtime.FromNanoseconds(compressedValues.GetStartTime())
	end := xtime.FromNanoseconds(compressedValues.GetEndTime())

	return encoding.NewSeriesIterator(id, ns, tagIter, start, end, allReplicaIterators, seriesPool), nil
}

// DecodeCompressedFetchResult decodes compressed fetch results to seriesIterators
func DecodeCompressedFetchResult(
	fetchResult *rpc.FetchResult,
	iteratorPools encoding.IteratorPools,
) (encoding.SeriesIterators, error) {
	rpcSeries := fetchResult.GetSeries()
	var (
		pooledIterators encoding.MutableSeriesIterators
		seriesIterators []encoding.SeriesIterator
		numSeries       = len(rpcSeries)
	)

	if iteratorPools != nil {
		seriesIteratorPool := iteratorPools.MutableSeriesIterators()
		pooledIterators = seriesIteratorPool.Get(numSeries)
		pooledIterators.Reset(numSeries)
	} else {
		seriesIterators = make([]encoding.SeriesIterator, numSeries)
	}
	for i, series := range rpcSeries {
		iter, err := seriesIteratorFromCompressedSeries(series, iteratorPools)
		if err != nil {
			return nil, err
		}
		if pooledIterators != nil {
			pooledIterators.SetAt(i, iter)
		} else {
			seriesIterators[i] = iter
		}
	}
	if pooledIterators != nil {
		return pooledIterators, nil
	}
	return encoding.NewSeriesIterators(seriesIterators, nil), nil
}
