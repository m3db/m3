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
	contextPool    context.Pool
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
	contextPool context.Pool,
	nsOpts namespace.Options,
) Merger {
	return &merger{
		reader:         reader,
		blockAllocSize: blockAllocSize,
		srPool:         srPool,
		multiIterPool:  multiIterPool,
		identPool:      identPool,
		encoderPool:    encoderPool,
		contextPool:    contextPool,
		nsOpts:         nsOpts,
	}
}

// Merge merges data from a fileset with a merge target and persists it.
// The caller is responsible for finalizing all resources used for the
// MergeWith passed here.
func (m *merger) Merge(
	fileID FileSetFileIdentifier,
	mergeWith MergeWith,
	nextVolumeIndex int,
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
		volume     = fileID.VolumeIndex
		blockSize  = nsOpts.RetentionOptions().BlockSize()
		blockStart = xtime.ToUnixNano(startTime)
		openOpts   = DataReaderOpenOptions{
			Identifier: FileSetFileIdentifier{
				Namespace:   nsID,
				Shard:       shard,
				BlockStart:  startTime,
				VolumeIndex: volume,
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
		VolumeIndex:       nextVolumeIndex,
		FileSetType:       persist.FileSetFlushType,
		DeleteIfExists:    false,
	}
	prepared, err := flushPreparer.PrepareData(prepareOpts)
	if err != nil {
		return err
	}

	var (
		// There will likely be at least two SegmentReaders - one for disk data and
		// one for data from the merge target.
		segmentReaders = make([]xio.SegmentReader, 0, 2)

		// It's safe to share these between iterations and just reset them each
		// time because the series gets persisted each loop, so the previous
		// iterations' reader and iterator will never be needed.
		segReader = srPool.Get()
		multiIter = multiIterPool.Get()
		ctx       = m.contextPool.Get()

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

		// Shared between iterations.
		iterResources = newIterResources(
			multiIter,
			blockStart.ToTime(),
			blockSize,
			blockAllocSize,
			nsCtx.Schema,
			encoderPool)
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
	for id, tagsIter, data, checksum, err := reader.Read(); err != io.EOF; id, tagsIter, data, checksum, err = reader.Read() {
		if err != nil {
			return err
		}
		idsToFinalize = append(idsToFinalize, id)

		segmentReaders = segmentReaders[:0]
		seg := segmentReaderFromData(data, checksum, segReader)
		segmentReaders = append(segmentReaders, seg)

		// Check if this series is in memory (and thus requires merging).
		ctx.Reset()
		mergeWithData, hasInMemoryData, err := mergeWith.Read(ctx, id, blockStart, nsCtx)
		if err != nil {
			return err
		}
		if hasInMemoryData {
			segmentReaders = appendBlockReadersToSegmentReaders(segmentReaders, mergeWithData)
		}

		// tagsIter is never nil. These tags will be valid as long as the IDs
		// are valid, and the IDs are valid for the duration of the file writing.
		tags, err := convert.TagsFromTagsIter(id, tagsIter, identPool)
		tagsIter.Close()
		if err != nil {
			return err
		}
		tagsToFinalize = append(tagsToFinalize, tags)

		// In the special (but common) case that we're just copying the series data from the old file
		// into the new one without merging or adding any additional data we can avoid recalculating
		// the checksum.
		if len(segmentReaders) == 1 && hasInMemoryData == false {
			segment, err := segmentReaders[0].Segment()
			if err != nil {
				return err
			}

			if err := persistSegmentWithChecksum(id, tags, segment, checksum, prepared.Persist); err != nil {
				return err
			}
		} else {
			if err := persistSegmentReaders(id, tags, segmentReaders, iterResources, prepared.Persist); err != nil {
				return err
			}
		}
		// Closing the context will finalize the data returned from
		// mergeWith.Read(), but is safe because it has already been persisted
		// to disk.
		// NB(r): Make sure to use BlockingCloseReset so can reuse the context.
		ctx.BlockingCloseReset()
	}
	// Second stage: loop through any series in the merge target that were not
	// captured in the first stage.
	ctx.Reset()
	err = mergeWith.ForEachRemaining(
		ctx, blockStart,
		func(id ident.ID, tags ident.Tags, mergeWithData []xio.BlockReader) error {
			segmentReaders = segmentReaders[:0]
			segmentReaders = appendBlockReadersToSegmentReaders(segmentReaders, mergeWithData)
			err := persistSegmentReaders(id, tags, segmentReaders, iterResources, prepared.Persist)
			// Context is safe to close after persisting data to disk.
			// Reset context here within the passed in function so that the
			// context gets reset for each remaining series instead of getting
			// finalized at the end of the ForEachRemaining call.
			// NB(r): Make sure to use BlockingCloseReset so can reuse the context.
			ctx.BlockingCloseReset()
			return err
		}, nsCtx)
	if err != nil {
		return err
	}

	// Close the flush preparer, which writes the rest of the files in the
	// fileset.
	return prepared.Close()
}

func appendBlockReadersToSegmentReaders(segReaders []xio.SegmentReader, brs []xio.BlockReader) []xio.SegmentReader {
	for _, br := range brs {
		segReaders = append(segReaders, br.SegmentReader)
	}
	return segReaders
}

func segmentReaderFromData(
	data checked.Bytes,
	checksum int64,
	segReader xio.SegmentReader,
) xio.SegmentReader {
	seg := ts.NewSegment(data, nil, checksum, ts.FinalizeHead)
	segReader.Reset(seg)
	return segReader
}

func persistSegmentReaders(
	id ident.ID,
	tags ident.Tags,
	segReaders []xio.SegmentReader,
	ir iterResources,
	persistFn persist.DataFn,
) error {
	if len(segReaders) == 0 {
		return nil
	}

	if len(segReaders) == 1 {
		return persistSegmentReader(id, tags, segReaders[0], persistFn)
	}

	return persistIter(id, tags, segReaders, ir, persistFn)
}

func persistIter(
	id ident.ID,
	tags ident.Tags,
	segReaders []xio.SegmentReader,
	ir iterResources,
	persistFn persist.DataFn,
) error {
	it := ir.multiIter
	it.Reset(segReaders, ir.blockStart, ir.blockSize, ir.schema)
	encoder := ir.encoderPool.Get()
	encoder.Reset(ir.blockStart, ir.blockAllocSize, ir.schema)
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
	return persistSegment(id, tags, segment, persistFn)
}

func persistSegmentReader(
	id ident.ID,
	tags ident.Tags,
	segmentReader xio.SegmentReader,
	persistFn persist.DataFn,
) error {
	segment, err := segmentReader.Segment()
	if err != nil {
		return err
	}
	return persistSegment(id, tags, segment, persistFn)
}

func persistSegment(
	id ident.ID,
	tags ident.Tags,
	segment ts.Segment,
	persistFn persist.DataFn,
) error {
	return persistFn(id, tags, segment, segment.Checksum)
}

func persistSegmentWithChecksum(
	id ident.ID,
	tags ident.Tags,
	segment ts.Segment,
	checksum uint32,
	persistFn persist.DataFn,
) error {
	return persistFn(id, tags, segment, checksum)
}

type iterResources struct {
	multiIter      encoding.MultiReaderIterator
	blockStart     time.Time
	blockSize      time.Duration
	blockAllocSize int
	schema         namespace.SchemaDescr
	encoderPool    encoding.EncoderPool
}

func newIterResources(
	multiIter encoding.MultiReaderIterator,
	blockStart time.Time,
	blockSize time.Duration,
	blockAllocSize int,
	schema namespace.SchemaDescr,
	encoderPool encoding.EncoderPool,
) iterResources {
	return iterResources{
		multiIter:      multiIter,
		blockStart:     blockStart,
		blockSize:      blockSize,
		blockAllocSize: blockAllocSize,
		schema:         schema,
		encoderPool:    encoderPool,
	}
}
