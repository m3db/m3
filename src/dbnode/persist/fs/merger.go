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
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type merger struct {
	reader         DataFileSetReader
	blockAllocSize int
	srPool         xio.SegmentReaderPool
	multiIterPool  encoding.MultiReaderIteratorPool
	identPool      ident.Pool
	encoderPool    encoding.EncoderPool
	nsOpts         namespace.Options
}

// NewMerger returns a new Merger. This implementation is in charge of merging
// the data from an existing fileset with a merge target. If data for a series
// at a timestamp exists both on disk and the merge target, data from the merge
// target will be used. This merged data is then persisted.
//
// Note that the merger does not know how or where this merged data is
// persisted since it just uses the flushPreparer that is passed in. Further,
// it does not signal to the database of the existence of the newly persisted
// data, nor does it clean up the original fileset.
func NewMerger(
	reader DataFileSetReader,
	blockAllocSize int,
	srPool xio.SegmentReaderPool,
	multiIterPool encoding.MultiReaderIteratorPool,
	identPool ident.Pool,
	encoderPool encoding.EncoderPool,
	nsOpts namespace.Options,
) Merger {
	return &merger{
		reader:         reader,
		blockAllocSize: blockAllocSize,
		srPool:         srPool,
		multiIterPool:  multiIterPool,
		identPool:      identPool,
		encoderPool:    encoderPool,
		nsOpts:         nsOpts,
	}
}

// Merge merges data from a fileset with a merge target and persists it.
// The caller is responsible for finalizing all resources used for the
// MergeWith passed here.
func (m *merger) Merge(
	fileID FileSetFileIdentifier,
	mergeWith MergeWith,
	flushPreparer persist.FlushPreparer,
	nsCtx namespace.Context,
) (err error) {
	var (
		reader         = m.reader
		blockAllocSize = m.blockAllocSize
		srPool         = m.srPool
		multiIterPool  = m.multiIterPool
		identPool      = m.identPool
		encoderPool    = m.encoderPool
		nsOpts         = m.nsOpts

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
			FileSetType: persist.FileSetFlushType,
		}
	)

	if err := reader.Open(openOpts); err != nil {
		return err
	}
	defer func() {
		// Only set the error here if not set by the end of the function, since
		// all other errors take precedence.
		if err == nil {
			err = reader.Close()
		}
	}()

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
		return err
	}

	var (
		// There will only be one BlockReader slice since we're working within one
		// block here.
		brs = make([][]xio.BlockReader, 0, 1)
		// There will likely be at least two BlockReaders - one for disk data and
		// one for data from the merge target.
		br = make([]xio.BlockReader, 0, 2)

		// It's safe to share these between iterations and just reset them each
		// time because the series gets persisted each loop, so the previous
		// iterations' reader and iterator will never be needed.
		segReader = srPool.Get()
		multiIter = multiIterPool.Get()
		// Initialize this here with nil to be reset before each iteration's
		// use.
		sliceOfSlices = xio.NewReaderSliceOfSlicesFromBlockReadersIterator(nil)
		// Reused context for use in mergeWith.Read, since they all do a
		// BlockingClose after usage.
		tmpCtx = context.NewContext()

		// We keep track of IDs/tags to finalize at the end of merging. This
		// only applies to those that come from disk Reads, since the whole
		// lifecycle of those IDs/tags are contained to this function. We don't
		// want finalize the IDs from memory since other components may have
		// ownership over it.
		//
		// We must only finalize these at the end of this function, since the
		// flush preparer's underlying writer holds on to those references
		// until it is closed (closing the PreparedDataPersist at the end of
		// this merge closes the underlying writer).
		idsToFinalize  = make([]ident.ID, 0, reader.Entries())
		tagsToFinalize = make([]ident.Tags, 0, reader.Entries())
	)
	defer func() {
		segReader.Finalize()
		multiIter.Close()
		for _, res := range idsToFinalize {
			res.Finalize()
		}
		for _, res := range tagsToFinalize {
			res.Finalize()
		}
	}()

	// The merge is performed in two stages. The first stage is to loop through
	// series on disk and merge it with what's in the merge target. Looping
	// through disk in the first stage is done intentionally to read disk
	// sequentially to optimize for spinning disk access. The second stage is to
	// persist the rest of the series in the merge target that were not
	// persisted in the first stage.

	// First stage: loop through series on disk.
	for id, tagsIter, data, _, err := reader.Read(); err != io.EOF; id, tagsIter, data, _, err = reader.Read() {
		idsToFinalize = append(idsToFinalize, id)
		if err != nil {
			return err
		}

		// Reset BlockReaders.
		brs = brs[:0]
		br = br[:0]
		br = append(br, blockReaderFromData(data, segReader, startTime, blockSize))

		// Check if this series is in memory (and thus requires merging).
		tmpCtx.Reset()
		mergeWithData, hasData, err := mergeWith.Read(tmpCtx, id, blockStart, nsCtx)
		if err != nil {
			return err
		}
		if hasData {
			br = append(br, mergeWithData...)
		}
		brs = append(brs, br)

		sliceOfSlices.Reset(brs)
		multiIter.ResetSliceOfSlices(sliceOfSlices, nsCtx.Schema)

		// tagsIter is never nil. These tags will be valid as long as the IDs
		// are valid, and the IDs are valid for the duration of the file writing.
		tags, err := convert.TagsFromTagsIter(id, tagsIter, identPool)
		tagsIter.Close()
		tagsToFinalize = append(tagsToFinalize, tags)
		if err != nil {
			return err
		}
		if err := persistIter(prepared.Persist, multiIter, startTime,
			id, tags, blockAllocSize, nsCtx.Schema, encoderPool); err != nil {
			return err
		}
		// Closing the context will finalize the data returned from
		// mergeWith.Read(), but is safe because it has already been persisted
		// to disk.
		tmpCtx.BlockingClose()
	}
	// Second stage: loop through any series in the merge target that were not
	// captured in the first stage.
	tmpCtx.Reset()
	err = mergeWith.ForEachRemaining(
		tmpCtx, blockStart,
		func(seriesID ident.ID, tags ident.Tags, mergeWithData []xio.BlockReader) error {
			brs = brs[:0]
			brs = append(brs, mergeWithData)
			sliceOfSlices.Reset(brs)
			multiIter.ResetSliceOfSlices(sliceOfSlices, nsCtx.Schema)
			err := persistIter(prepared.Persist, multiIter, startTime,
				seriesID, tags, blockAllocSize, nsCtx.Schema, encoderPool)
			// Context is safe to close after persisting data to disk.
			tmpCtx.BlockingClose()
			// Reset context here within the passed in function so that the
			// context gets reset for each remaining series instead of getting
			// finalized at the end of the ForEachRemaining call.
			tmpCtx.Reset()
			return err
		}, nsCtx)
	if err != nil {
		return err
	}

	// Close the flush preparer, which writes the rest of the files in the
	// fileset.
	return prepared.Close()
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
	blockStart time.Time,
	id ident.ID,
	tags ident.Tags,
	blockAllocSize int,
	schema namespace.SchemaDescr,
	encoderPool encoding.EncoderPool,
) error {
	encoder := encoderPool.Get()
	encoder.Reset(blockStart, blockAllocSize, schema)
	for it.Next() {
		if err := encoder.Encode(it.Current()); err != nil {
			encoder.Close()
			return err
		}
	}
	if err := it.Err(); err != nil {
		encoder.Close()
		return err
	}

	segment := encoder.Discard()
	checksum := digest.SegmentChecksum(segment)

	return persistFn(id, tags, segment, checksum)
}
