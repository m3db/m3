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

package storage

import (
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type fsMergerReusableResources struct {
	fsReader      fs.DataFileSetReader
	srPool        xio.SegmentReaderPool
	multiIterPool encoding.MultiReaderIteratorPool
	identPool     ident.Pool
	encoderPool   encoding.EncoderPool
}

type fsMerger struct {
	res fsMergerReusableResources
}

func (m *fsMerger) Merge(
	ns namespace.Metadata,
	shard uint32,
	blockStart xtime.UnixNano,
	blockSize time.Duration,
	flushPreparer persist.FlushPreparer,
	mergeWith FsMergeWith,
) error {
	var multiErr xerrors.MultiError

	fsReader := m.res.fsReader
	srPool := m.res.srPool
	multiIterPool := m.res.multiIterPool
	identPool := m.res.identPool
	encoderPool := m.res.encoderPool

	startTime := blockStart.ToTime()
	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  ns.ID(),
			Shard:      shard,
			BlockStart: startTime,
		},
	}
	if err := fsReader.Open(openOpts); err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}
	defer fsReader.Close()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: ns,
		Shard:             shard,
		BlockStart:        startTime,
		DeleteIfExists:    false,
	}
	prepared, err := flushPreparer.PrepareData(prepareOpts)
	if err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}

	// Writing data for a block is done in two stages. The first stage is
	// to loop through series on disk and merge it with what's in the merge
	// target. The second stage is to persist the rest of the series in the
	// merge target that was not persisted in the first stage.

	// First stage: loop through series on disk.
	for id, tagsIter, data, _, err := fsReader.Read(); err != io.EOF; id, tagsIter, data, _, err = fsReader.Read() {
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}

		// There will be at most two BlockReader slices: one for disk data
		// and one for in memory data.
		brs := make([][]xio.BlockReader, 0, 2)

		// Create BlockReader slice out of disk data.
		seg := ts.NewSegment(data, nil, ts.FinalizeHead)
		sr := srPool.Get()
		sr.Reset(seg)
		br := xio.BlockReader{
			SegmentReader: sr,
			Start:         startTime,
			BlockSize:     blockSize,
		}
		brs = append(brs, []xio.BlockReader{br})

		// Check if this series is in memory (and thus requires merging).
		encoded, hasData, err := mergeWith.Read(blockStart, id)
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}

		if hasData {
			brs = append(brs, encoded)
		}

		mergedIter := multiIterPool.Get()
		mergedIter.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromBlockReadersIterator(brs))
		defer mergedIter.Close()

		tags, err := convert.TagsFromTagsIter(id, tagsIter, identPool)
		tagsIter.Close()
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}
		if err := persistIter(prepared.Persist, mergedIter, id, tags, encoderPool); err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}
	}

	// Second stage: loop through rest of the merge target that was not captured
	// in the first stage.
	err = mergeWith.ForEachRemaining(blockStart, func(seriesID ident.ID, tags ident.Tags) bool {
		encoded, hasData, err := mergeWith.Read(blockStart, seriesID)
		if err != nil {
			multiErr = multiErr.Add(err)
			return false
		}

		if hasData {
			iter := multiIterPool.Get()
			iter.Reset(xio.NewSegmentReaderSliceFromBlockReaderSlice(encoded), startTime, blockSize)
			defer iter.Close()
			if err := persistIter(prepared.Persist, iter, seriesID, tags, encoderPool); err != nil {
				multiErr = multiErr.Add(err)
				return false
			}
		}

		return true
	})
	if err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}

	// Close the flush preparer, which writes the rest of the files in the
	// fileset.
	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}

	return multiErr.FinalError()
}

func persistIter(
	persistFn persist.DataFn,
	it encoding.Iterator,
	id ident.ID,
	tags ident.Tags,
	encoderPool encoding.EncoderPool,
) error {
	encoder := encoderPool.Get()
	for it.Next() {
		if err := encoder.Encode(it.Current()); err != nil {
			encoder.Close()
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}

	stream := encoder.Stream()
	encoder.Close()

	segment, err := stream.Segment()
	if err != nil {
		return err
	}

	checksum := digest.SegmentChecksum(segment)

	err = persistFn(id, tags, segment, checksum)
	id.Finalize()
	tags.Finalize()
	if err != nil {
		return err
	}

	return nil
}
