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
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	queryerrors "github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errDecodeNoIteratorPools = errors.New("no iterator pools for decoding")
)

func compressedSegmentFromBlockReader(br xio.BlockReader) (*rpc.M3Segment, error) {
	segment, err := br.Segment()
	if err != nil {
		return nil, err
	}

	return &rpc.M3Segment{
		Head:      segment.Head.Bytes(),
		Tail:      segment.Tail.Bytes(),
		StartTime: int64(br.Start),
		BlockSize: int64(br.BlockSize),
		Checksum:  segment.CalculateChecksum(),
	}, nil
}

func compressedSegmentsFromReaders(
	readers xio.ReaderSliceOfSlicesIterator,
) (*rpc.M3Segments, error) {
	segments := &rpc.M3Segments{}
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
		unmerged := make([]*rpc.M3Segment, 0, l)
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

func compressedTagsFromTagIterator(
	tagIter ident.TagIterator,
	encoderPool serialize.TagEncoderPool,
) ([]byte, error) {
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

	db := data.Bytes()
	// Need to copy the encoded bytes to a buffer as the encoder keeps a reference to them
	// TODO(arnikola): pool this when implementing https://github.com/m3db/m3/issues/1015
	return append(make([]byte, 0, len(db)), db...), nil
}

func buildTags(tagIter ident.TagIterator, iterPools encoding.IteratorPools) ([]byte, error) {
	if iterPools != nil {
		encoderPool := iterPools.TagEncoder()
		if encoderPool != nil {
			return compressedTagsFromTagIterator(tagIter, encoderPool)
		}
	}

	return nil, queryerrors.ErrCannotEncodeCompressedTags
}

// CompressedSeriesFromSeriesIterator builds compressed rpc series from a SeriesIterator
// SeriesIterator is the top level iterator returned by m3db
func CompressedSeriesFromSeriesIterator(
	it encoding.SeriesIterator,
	iterPools encoding.IteratorPools,
) (*rpc.Series, error) {
	// This SeriesIterator contains MultiReaderIterators, each representing a single
	// replica. Each MultiReaderIterator has a ReaderSliceOfSlicesIterator where each
	// step through the iterator exposes a slice of underlying BlockReaders. Each
	// BlockReader contains the run time encoded bytes that represent the series.
	//
	// SeriesIterator also has a TagIterator representing the tags associated with it.
	//
	// This function transforms a SeriesIterator into a protobuf representation to be
	// able to send it across the wire without needing to expand the series.
	//
	// If reset argument is true, the SeriesIterator readers will be reset so it can
	// be iterated again. If false, the SeriesIterator will no longer be useable.
	replicas, err := it.Replicas()
	if err != nil {
		return nil, err
	}

	compressedReplicas := make([]*rpc.M3CompressedValuesReplica, 0, len(replicas))
	for _, replica := range replicas {
		replicaSegments := make([]*rpc.M3Segments, 0, len(replicas))
		readers := replica.Readers()
		idx := readers.Index()
		for next := true; next; next = readers.Next() {
			segments, err := compressedSegmentsFromReaders(readers)
			if err != nil {
				return nil, err
			}
			replicaSegments = append(replicaSegments, segments)
		}

		// Restore the original index of the reader so the caller can resume
		// the iterator at the expected state. This is safe because we do not
		// consume any of the internal block readers within the iterator.
		// It cannot be asserted that iters are passed in here at idx 0 which is
		// why we make sure to rewind to the specific original index.
		readers.RewindToIndex(idx)

		r := &rpc.M3CompressedValuesReplica{
			Segments: replicaSegments,
		}
		compressedReplicas = append(compressedReplicas, r)
	}

	start := int64(it.Start())
	end := int64(it.End())

	itTags := it.Tags()
	defer itTags.Rewind()
	tags, err := buildTags(itTags, iterPools)
	if err != nil {
		return nil, err
	}

	return &rpc.Series{
		Meta: &rpc.SeriesMetadata{
			Id:        it.ID().Bytes(),
			StartTime: start,
			EndTime:   end,
		},
		Value: &rpc.Series_Compressed{
			Compressed: &rpc.M3CompressedSeries{
				CompressedTags: tags,
				Replicas:       compressedReplicas,
			},
		},
	}, nil
}

// encodeToCompressedSeries encodes SeriesIterators to compressed series.
func encodeToCompressedSeries(
	results consolidators.SeriesFetchResult,
	iterPools encoding.IteratorPools,
) ([]*rpc.Series, error) {
	iters := results.SeriesIterators()
	seriesList := make([]*rpc.Series, 0, len(iters))
	for _, iter := range iters {
		series, err := CompressedSeriesFromSeriesIterator(iter, iterPools)
		if err != nil {
			return nil, err
		}

		seriesList = append(seriesList, series)
	}

	return seriesList, nil
}

func segmentBytesFromCompressedSegment(
	segHead, segTail []byte,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) (checked.Bytes, checked.Bytes) {
	return checkedBytesWrapperPool.Get(segHead), checkedBytesWrapperPool.Get(segTail)
}

func blockReaderFromCompressedSegment(
	seg *rpc.M3Segment,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) xio.BlockReader {
	head, tail := segmentBytesFromCompressedSegment(seg.GetHead(), seg.GetTail(), checkedBytesWrapperPool)
	segment := ts.NewSegment(head, tail, seg.GetChecksum(), ts.FinalizeNone)
	segmentReader := xio.NewSegmentReader(segment)

	return xio.BlockReader{
		SegmentReader: segmentReader,
		Start:         xtime.UnixNano(seg.GetStartTime()),
		BlockSize:     time.Duration(seg.GetBlockSize()),
	}
}

func tagIteratorFromCompressedTagsWithDecoder(
	compressedTags []byte,
	iterPools encoding.IteratorPools,
) (ident.TagIterator, error) {
	if iterPools == nil || iterPools.CheckedBytesWrapper() == nil || iterPools.TagDecoder() == nil {
		return nil, queryerrors.ErrCannotDecodeCompressedTags
	}

	checkedBytes := iterPools.CheckedBytesWrapper().Get(compressedTags)
	decoder := iterPools.TagDecoder().Get()
	decoder.Reset(checkedBytes)
	defer decoder.Close()
	// Copy underlying TagIterator bytes before closing the decoder and returning it to the pool
	return decoder.Duplicate(), nil
}

func tagIteratorFromSeries(
	series *rpc.M3CompressedSeries,
	iteratorPools encoding.IteratorPools,
) (ident.TagIterator, error) {
	if series != nil && len(series.GetCompressedTags()) > 0 {
		return tagIteratorFromCompressedTagsWithDecoder(
			series.GetCompressedTags(),
			iteratorPools,
		)
	}

	return iteratorPools.TagDecoder().Get().Duplicate(), nil
}

func blockReadersFromCompressedSegments(
	segments []*rpc.M3Segments,
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool,
) [][]xio.BlockReader {
	blockReaders := make([][]xio.BlockReader, len(segments))

	for i, segment := range segments {
		blockReadersPerSegment := make([]xio.BlockReader, 0, len(segments))
		mergedSegment := segment.GetMerged()
		if mergedSegment != nil {
			reader := blockReaderFromCompressedSegment(mergedSegment, checkedBytesWrapperPool)
			blockReadersPerSegment = append(blockReadersPerSegment, reader)
		} else {
			unmerged := segment.GetUnmerged()
			for _, seg := range unmerged {
				reader := blockReaderFromCompressedSegment(seg, checkedBytesWrapperPool)
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
argument that allows reuse of the underlying iterator pools from the m3db session.
*/
func seriesIteratorFromCompressedSeries(
	timeSeries *rpc.M3CompressedSeries,
	meta *rpc.SeriesMetadata,
	iteratorPools encoding.IteratorPools,
) (encoding.SeriesIterator, error) {
	// NB: Attempt to decompress compressed tags first as this is the only scenario
	// that is expected to fail.
	tagIter, err := tagIteratorFromSeries(timeSeries, iteratorPools)
	if err != nil {
		return nil, err
	}

	replicas := timeSeries.GetReplicas()

	multiReaderPool := iteratorPools.MultiReaderIterator()
	seriesIterPool := iteratorPools.SeriesIterator()
	checkedBytesWrapperPool := iteratorPools.CheckedBytesWrapper()
	idPool := iteratorPools.ID()

	allReplicaIterators := iteratorPools.MultiReaderIteratorArray().Get(len(replicas))

	for _, replica := range replicas {
		blockReaders := blockReadersFromCompressedSegments(replica.GetSegments(), checkedBytesWrapperPool)

		// TODO arnikola investigate pooling these?
		sliceOfSlicesIterator := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(blockReaders)
		perReplicaIterator := multiReaderPool.Get()
		perReplicaIterator.ResetSliceOfSlices(sliceOfSlicesIterator, nil)

		allReplicaIterators = append(allReplicaIterators, perReplicaIterator)
	}

	var (
		id, ns ident.ID
	)
	idBytes := checkedBytesWrapperPool.Get(meta.GetId())
	id = idPool.BinaryID(idBytes)
	start := xtime.UnixNano(meta.GetStartTime())
	end := xtime.UnixNano(meta.GetEndTime())

	seriesIter := seriesIterPool.Get()
	seriesIter.Reset(encoding.SeriesIteratorOptions{
		ID:             id,
		Namespace:      ns,
		Tags:           tagIter,
		StartInclusive: start,
		EndExclusive:   end,
		Replicas:       allReplicaIterators,
	})

	return seriesIter, nil
}

// DecodeCompressedFetchResponse decodes compressed fetch
// response to seriesIterators.
func DecodeCompressedFetchResponse(
	fetchResult *rpc.FetchResponse,
	iteratorPools encoding.IteratorPools,
) (encoding.SeriesIterators, error) {
	if iteratorPools == nil {
		return nil, errDecodeNoIteratorPools
	}

	var (
		seriesIteratorPool = iteratorPools.MutableSeriesIterators()
		rpcSeries          = fetchResult.GetSeries()
		numSeries          = len(rpcSeries)
	)

	iters := seriesIteratorPool.Get(numSeries)
	iters.Reset(numSeries)

	for i, series := range rpcSeries {
		compressed := series.GetCompressed()
		if compressed == nil {
			continue
		}

		iter, err := seriesIteratorFromCompressedSeries(
			compressed,
			series.GetMeta(),
			iteratorPools,
		)
		if err != nil {
			return nil, err
		}

		iters.SetAt(i, iter)
	}

	return iters, nil
}
