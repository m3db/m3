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

package fs

import (
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Merger is in charge of merging filesets with some target MergeWith interface.
type Merger struct {
	reader        DataFileSetReader
	srPool        xio.SegmentReaderPool
	multiIterPool encoding.MultiReaderIteratorPool
	identPool     ident.Pool
	encoderPool   encoding.EncoderPool
}

// NewMerger returns a new Merger.
func NewMerger(
	reader DataFileSetReader,
	srPool xio.SegmentReaderPool,
	multiIterPool encoding.MultiReaderIteratorPool,
	identPool ident.Pool,
	encoderPool encoding.EncoderPool,
) *Merger {
	return &Merger{
		reader:        reader,
		srPool:        srPool,
		multiIterPool: multiIterPool,
		identPool:     identPool,
		encoderPool:   encoderPool,
	}
}

// Merge merges data from a fileset with a merge target and persists it.
func (m *Merger) Merge(
	fileID FileSetFileIdentifier,
	mergeWith MergeWith,
	flushPreparer persist.FlushPreparer,
	nsOpts namespace.Options,
	nsCtx namespace.Context,
) error {
	var (
		multiErr      xerrors.MultiError
		reader        = m.reader
		srPool        = m.srPool
		multiIterPool = m.multiIterPool
		identPool     = m.identPool
		encoderPool   = m.encoderPool

		nsID       = fileID.Namespace
		shard      = fileID.Shard
		startTime  = fileID.BlockStart
		blockSize  = nsOpts.RetentionOptions().BlockSize()
		blockStart = xtime.ToUnixNano(startTime)
		openOpts   = DataReaderOpenOptions{
			Identifier: FileSetFileIdentifier{
				Namespace:  nsID,
				Shard:      shard,
				BlockStart: startTime,
			},
		}
	)

	if err := reader.Open(openOpts); err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}
	defer reader.Close()

	nsMd, err := namespace.NewMetadata(nsID, nsOpts)
	if err != nil {
		return err
	}
	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: nsMd,
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
	// target. Looping through disk in the first stage is done intentionally to
	// read disk sequentially to optimize for spinning disk access.
	// The second stage is to persist the rest of the series in the
	// merge target that were not persisted in the first stage.

	// There will only be one BlockReader slice since we're working within one
	// block here.
	brs := make([][]xio.BlockReader, 0, 1)
	// There will likely be at least two BlockReaders - one for disk data and
	// one for data from the merge target.
	br := make([]xio.BlockReader, 0, 2)

	// It's safe to share these between iterations and just reset them each time
	// because the series gets persisted each loop, so previous iterations'
	// reader and iterator will never be needed.
	segReader := srPool.Get()
	defer segReader.Finalize()
	multiIter := multiIterPool.Get()
	defer multiIter.Close()

	// First stage: loop through series on disk.
	for id, tagsIter, data, _, err := reader.Read(); err != io.EOF; id, tagsIter, data, _, err = reader.Read() {
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}

		// Reset BlockReaders.
		brs = brs[:0]
		br = br[:0]
		br = append(br, blockReaderFromData(data, segReader, startTime, blockSize))

		// Check if this series is in memory (and thus requires merging).
		encoded, hasData, err := mergeWith.Read(id, blockStart, nsCtx)
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}
		if hasData {
			br = append(br, encoded...)
		}
		brs = append(brs, br)

		multiIter.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromBlockReadersIterator(brs), nsCtx.Schema)

		// tagsIter is never nil.
		tags, err := convert.TagsFromTagsIter(id, tagsIter, identPool)
		tagsIter.Close()
		if err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}
		if err := persistIter(prepared.Persist, multiIter, id, tags, encoderPool); err != nil {
			multiErr = multiErr.Add(err)
			return multiErr.FinalError()
		}
	}
	// Second stage: loop through rest of the merge target that was not captured
	// in the first stage.
	err = mergeWith.ForEachRemaining(blockStart, func(seriesID ident.ID, tags ident.Tags) bool {
		encoded, hasData, err := mergeWith.Read(seriesID, blockStart, nsCtx)
		if err != nil {
			multiErr = multiErr.Add(err)
			return false
		}

		if hasData {
			multiIter.Reset(xio.NewSegmentReaderSliceFromBlockReaderSlice(encoded), startTime, blockSize, nsCtx.Schema)
			if err := persistIter(prepared.Persist, multiIter, seriesID, tags, encoderPool); err != nil {
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

func blockReaderFromData(
	data checked.Bytes,
	segReader xio.SegmentReader,
	startTime time.Time,
	blockSize time.Duration,
) xio.BlockReader {
	seg := ts.NewSegment(data, nil, ts.FinalizeHead)
	segReader.Reset(seg)
	return xio.BlockReader{
		SegmentReader: segReader,
		Start:         startTime,
		BlockSize:     blockSize,
	}
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

	segment := encoder.Discard()
	checksum := digest.SegmentChecksum(segment)

	err := persistFn(id, tags, segment, checksum)
	id.Finalize()
	tags.Finalize()
	if err != nil {
		return err
	}

	return nil
}
