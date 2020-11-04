// Copyright (c) 2016 Uber Technologies, Inc.
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

package generate

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	ns "github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errInvalidFileSetType = errors.New("invalid file set type")
)

type writer struct {
	opts Options
}

// WriteAllPredicate writes all datapoints
func WriteAllPredicate(_ TestValue) bool {
	return true
}

// NewWriter returns a new writer
func NewWriter(opts Options) Writer {
	return &writer{
		opts: opts,
	}
}

func (w *writer) WriteData(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	volume int,
) error {
	return w.WriteDataWithPredicate(nsCtx, shardSet, seriesMaps, volume, WriteAllPredicate)
}

func (w *writer) WriteSnapshot(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	volume int,
	snapshotInterval time.Duration,
) error {
	return w.WriteSnapshotWithPredicate(
		nsCtx, shardSet, seriesMaps, volume, WriteAllPredicate, snapshotInterval)
}

func (w *writer) WriteIndex(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
) error {
	return w.WriteIndexWithPredicate(
		nsCtx, shardSet, seriesMaps, WriteAllPredicate)
}

func (w *writer) WriteIndexWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	pred WriteDatapointPredicate,
) error {
	return w.writeIndexWithPredicate(
		nsCtx, shardSet, seriesMaps, pred, persist.FileSetFlushType, 0)
}

func (w *writer) WriteIndexSnapshotWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	pred WriteDatapointPredicate,
	snapshotInterval time.Duration,
) error {
	return w.writeIndexWithPredicate(
		nsCtx, shardSet, seriesMaps, pred, persist.FileSetSnapshotType, snapshotInterval)
}

func (w *writer) WriteDataWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	volume int,
	pred WriteDatapointPredicate,
) error {
	return w.writeWithPredicate(
		nsCtx, shardSet, seriesMaps, volume, pred, persist.FileSetFlushType, 0)
}

func (w *writer) WriteSnapshotWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	volume int,
	pred WriteDatapointPredicate,
	snapshotInterval time.Duration,
) error {
	return w.writeWithPredicate(
		nsCtx, shardSet, seriesMaps, volume, pred, persist.FileSetSnapshotType, snapshotInterval)
}

func (w *writer) writeWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	volume int,
	pred WriteDatapointPredicate,
	fileSetType persist.FileSetType,
	snapshotInterval time.Duration,
) error {
	var (
		gOpts          = w.opts
		blockSize      = gOpts.BlockSize()
		currStart      = gOpts.ClockOptions().NowFn()().Truncate(blockSize)
		retentionStart = currStart.Add(-gOpts.RetentionPeriod())
		isValidStart   = func(start time.Time) bool {
			return start.Equal(retentionStart) || start.After(retentionStart)
		}
		starts = make(map[xtime.UnixNano]struct{})
	)

	for start := currStart; isValidStart(start); start = start.Add(-blockSize) {
		starts[xtime.ToUnixNano(start)] = struct{}{}
	}

	writer, err := fs.NewWriter(fs.NewOptions().
		SetFilePathPrefix(gOpts.FilePathPrefix()).
		SetWriterBufferSize(gOpts.WriterBufferSize()).
		SetNewFileMode(gOpts.NewFileMode()).
		SetNewDirectoryMode(gOpts.NewDirectoryMode()))
	if err != nil {
		return err
	}
	encoder := gOpts.EncoderPool().Get()
	encoder.SetSchema(nsCtx.Schema)
	for start, data := range seriesMaps {
		err := writeToDiskWithPredicate(
			writer, shardSet, encoder, start.ToTime(), nsCtx, blockSize,
			data, volume, pred, fileSetType, snapshotInterval)
		if err != nil {
			return err
		}
		delete(starts, start)
	}

	// Write remaining files even for empty start periods to avoid unfulfilled ranges
	if w.opts.WriteEmptyShards() {
		for start := range starts {
			err := writeToDiskWithPredicate(
				writer, shardSet, encoder, start.ToTime(), nsCtx, blockSize,
				nil, volume, pred, fileSetType, snapshotInterval)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *writer) writeIndexWithPredicate(
	nsCtx ns.Context,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	pred WriteDatapointPredicate,
	fileSetType persist.FileSetType,
	snapshotInterval time.Duration,
) error {
	gOpts := w.opts
	writer, err := fs.NewIndexWriter(fs.NewOptions().
		SetFilePathPrefix(gOpts.FilePathPrefix()).
		SetWriterBufferSize(gOpts.WriterBufferSize()).
		SetNewFileMode(gOpts.NewFileMode()).
		SetNewDirectoryMode(gOpts.NewDirectoryMode()))
	if err != nil {
		return err
	}
	shardsMap := make(map[uint32]struct{})
	for _, shard := range shardSet.AllIDs() {
		shardsMap[shard] = struct{}{}
	}
	// Convert series maps to series per block after applying predicate.
	docsPerBlockStart := make(map[xtime.UnixNano][]doc.Document)
	for start, data := range seriesMaps {
		for _, series := range data {
			var found bool
			for _, dp := range series.Data {
				if pred(dp) {
					found = true
					break
				}
			}
			if found {
				doc, err := convert.FromSeriesIDAndTags(series.ID, series.Tags)
				if err != nil {
					return err
				}
				docsPerBlockStart[start] = append(docsPerBlockStart[start], doc)
			}
		}
	}

	var (
		indexBlockSize = gOpts.IndexBlockSize()
		currStart      = xtime.ToUnixNano(gOpts.ClockOptions().NowFn()().Truncate(indexBlockSize))
	)
	for start, docs := range docsPerBlockStart {
		var indexVolumeType idxpersist.IndexVolumeType
		switch fileSetType {
		case persist.FileSetFlushType:
			indexVolumeType = idxpersist.DefaultIndexVolumeType
		case persist.FileSetSnapshotType:
			indexVolumeType = idxpersist.SnapshotColdIndexVolumeType
			if start.Equal(currStart) || start.After(currStart) {
				indexVolumeType = idxpersist.SnapshotWarmIndexVolumeType
			}
		default:
			return errInvalidFileSetType
		}

		if err := writeIndexToDisk(
			nsCtx,
			writer,
			shardsMap,
			indexBlockSize,
			start.ToTime(),
			snapshotInterval,
			gOpts.FilePathPrefix(),
			indexVolumeType,
			fileSetType,
			docs,
		); err != nil {
			return err
		}
	}

	// No-op for empty start periods since commit log bootstrap fulfills all requested ranges if successful.
	return nil
}

func writeToDiskWithPredicate(
	writer fs.DataFileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	nsCtx ns.Context,
	blockSize time.Duration,
	seriesList SeriesBlock,
	volume int,
	pred WriteDatapointPredicate,
	fileSetType persist.FileSetType,
	snapshotInterval time.Duration,
) error {
	seriesPerShard := make(map[uint32][]Series)
	for _, shard := range shardSet.AllIDs() {
		// Ensure we write out block files for each shard even if there's no data
		seriesPerShard[shard] = make([]Series, 0)
	}
	for _, s := range seriesList {
		shard := shardSet.Lookup(s.ID)
		seriesPerShard[shard] = append(seriesPerShard[shard], s)
	}
	data := make([]checked.Bytes, 2)
	for shard, seriesList := range seriesPerShard {
		writerOpts := fs.DataWriterOpenOptions{
			BlockSize: blockSize,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   nsCtx.ID,
				Shard:       shard,
				BlockStart:  start,
				VolumeIndex: volume,
			},
			FileSetType: fileSetType,
			Snapshot: fs.DataWriterSnapshotOptions{
				SnapshotTime: start.Add(snapshotInterval),
			},
		}

		if err := writer.Open(writerOpts); err != nil {
			return err
		}

		ctx := context.NewContext()
		for _, series := range seriesList {
			encoder.Reset(start, 0, nsCtx.Schema)
			for _, dp := range series.Data {
				if !pred(dp) {
					continue
				}

				if err := encoder.Encode(dp.Datapoint, xtime.Second, dp.Annotation); err != nil {
					return err
				}
			}

			ctx.Reset()
			stream, ok := encoder.Stream(ctx)
			if !ok {
				// None of the datapoints passed the predicate.
				continue
			}
			segment, err := stream.Segment()
			if err != nil {
				return err
			}
			data[0] = segment.Head
			data[1] = segment.Tail
			checksum := segment.CalculateChecksum()
			metadata := persist.NewMetadataFromIDAndTags(series.ID, series.Tags,
				persist.MetadataOptions{})
			err = writer.WriteAll(metadata, data, checksum)
			if err != nil {
				return err
			}
			ctx.BlockingClose()
		}

		if err := writer.Close(); err != nil {
			return err
		}
	}

	return nil
}

func writeIndexToDisk(
	nsCtx ns.Context,
	writer fs.IndexFileSetWriter,
	shardsMap map[uint32]struct{},
	blockSize time.Duration,
	start time.Time,
	snapshotInterval time.Duration,
	filePathPrefix string,
	indexVolumeType idxpersist.IndexVolumeType,
	fileSetType persist.FileSetType,
	docs []doc.Document,
) error {
	volumeIndex, err := fs.NextIndexFileSetVolumeIndex(
		filePathPrefix,
		nsCtx.ID,
		start,
	)
	if err != nil {
		return err
	}
	writerOpts := fs.IndexWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:          nsCtx.ID,
			BlockStart:         start,
			VolumeIndex:        volumeIndex,
			FileSetContentType: persist.FileSetIndexContentType,
		},
		FileSetType:     fileSetType,
		BlockSize:       blockSize,
		Shards:          shardsMap,
		IndexVolumeType: indexVolumeType,
		Snapshot: fs.IndexWriterSnapshotOptions{
			SnapshotTime: start.Add(snapshotInterval),
		},
	}
	if err := writer.Open(writerOpts); err != nil {
		return err
	}
	segmentWriter, err := idxpersist.NewMutableSegmentFileSetWriter(fst.WriterOptions{})
	if err != nil {
		return err
	}
	builder, err := builder.NewBuilderFromDocuments(builder.NewOptions())
	for _, doc := range docs {
		_, err = builder.Insert(doc)
		if err != nil {
			return err
		}
	}
	if err := segmentWriter.Reset(builder); err != nil {
		return err
	}
	if err := writer.WriteSegmentFileSet(segmentWriter); err != nil {
		return err
	}
	if err := builder.Close(); err != nil {
		return err
	}
	return writer.Close()
}
