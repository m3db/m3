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
	"fmt"
	"time"

	"github.com/m3db/m3db/src/dbnode/digest"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/sharding"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

type writer struct {
	opts Options
}

// NewWriter returns a new writer
func NewWriter(opts Options) Writer {
	return &writer{
		opts: opts,
	}
}

func (w *writer) Write(namespace ident.ID, shardSet sharding.ShardSet, seriesMaps SeriesBlocksByStart) error {
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
		err := writeToDisk(writer, shardSet, encoder, start.ToTime(), namespace, blockSize, data)
		if err != nil {
			return err
		}
		delete(starts, start)
	}

	// Write remaining files even for empty start periods to avoid unfulfilled ranges
	if w.opts.WriteEmptyShards() {
		for start := range starts {
			err := writeToDisk(writer, shardSet, encoder, start.ToTime(), namespace, blockSize, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func writeToDisk(
	writer fs.DataFileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	namespace ident.ID,
	blockSize time.Duration,
	seriesList SeriesBlock,
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
		}
		if err := writer.Open(writerOpts); err != nil {
			return err
		}
		for _, series := range seriesList {
			encoder.Reset(start, 0)
			for _, dp := range series.Data {
				if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
					return err
				}
			}
			stream := encoder.Stream()
			if stream == nil {
				return fmt.Errorf("nil stream")
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
