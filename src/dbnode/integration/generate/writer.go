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
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type writer struct {
	opts Options
}

// WriteAllPredicate writes all datapoints
func WriteAllPredicate(_ ts.Datapoint) bool {
	return true
}

// NewWriter returns a new writer
func NewWriter(opts Options) Writer {
	return &writer{
		opts: opts,
	}
}

func (w *writer) WriteData(
	namespace ident.ID,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
) error {
	return w.WriteDataWithPredicate(namespace, shardSet, seriesMaps, WriteAllPredicate)
}

func (w *writer) WriteSnapshot(
	namespace ident.ID,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	snapshotInterval time.Duration,
) error {
	return w.WriteSnapshotWithPredicate(
		namespace, shardSet, seriesMaps, WriteAllPredicate, snapshotInterval)
}

func (w *writer) WriteDataWithPredicate(
	namespace ident.ID,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	pred WriteDatapointPredicate,
) error {
	return w.writeWithPredicate(
		namespace, shardSet, seriesMaps, pred, persist.FileSetFlushType, 0)
}

func (w *writer) WriteSnapshotWithPredicate(
	namespace ident.ID,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
	pred WriteDatapointPredicate,
	snapshotInterval time.Duration,
) error {
	return w.writeWithPredicate(
		namespace, shardSet, seriesMaps, pred, persist.FileSetSnapshotType, snapshotInterval)
}

func (w *writer) writeWithPredicate(
	namespace ident.ID,
	shardSet sharding.ShardSet,
	seriesMaps SeriesBlocksByStart,
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
	for start, data := range seriesMaps {
		err := writeToDiskWithPredicate(
			writer, shardSet, encoder, start.ToTime(), namespace, blockSize,
			data, pred, fileSetType, snapshotInterval)
		if err != nil {
			return err
		}
		delete(starts, start)
	}

	// Write remaining files even for empty start periods to avoid unfulfilled ranges
	if w.opts.WriteEmptyShards() {
		for start := range starts {
			err := writeToDiskWithPredicate(
				writer, shardSet, encoder, start.ToTime(), namespace, blockSize,
				nil, pred, fileSetType, snapshotInterval)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func writeToDiskWithPredicate(
	writer fs.DataFileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	namespace ident.ID,
	blockSize time.Duration,
	seriesList SeriesBlock,
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
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: start,
			},
			FileSetType: fileSetType,
			Snapshot: fs.DataWriterSnapshotOptions{
				SnapshotTime: start.Add(snapshotInterval),
			},
		}
		if err := writer.Open(writerOpts); err != nil {
			return err
		}
		for _, series := range seriesList {
			encoder.Reset(start, 0)
			for _, dp := range series.Data {
				if !pred(dp) {
					continue
				}

				if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
					return err
				}
			}

			stream, ok := encoder.Stream(encoding.StreamOptions{})
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
			checksum := digest.SegmentChecksum(segment)
			err = writer.WriteAll(series.ID, series.Tags, data, checksum)
			if err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
	}

	return nil
}
