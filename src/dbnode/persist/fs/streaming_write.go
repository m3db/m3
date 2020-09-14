// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/m3db/bloom/v4"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
)

// StreamingWriter writes int data fileset without intermediate buffering.
// Writes must be lexicographically ordered by the id.
type StreamingWriter interface {
	Open() error
	Write(ctx context.Context, encoder encoding.Encoder, id ident.ID, encodedTags []byte) error
	Close() error
}

// StreamingWriterOptions in the options for the StreamingWriter.
type StreamingWriterOptions struct {
	Options             Options
	NamespaceID         ident.ID
	ShardID             uint32
	BlockStart          time.Time
	BlockSize           time.Duration
	VolumeIndex         int
	PlannedRecordsCount uint
}

type streamingWriter struct {
	opts         StreamingWriterOptions
	writer       *writer
	writerOpts   DataWriterOpenOptions
	data         []checked.Bytes
	currIdx      int64
	prevIDBytes  []byte
	summaryEvery int64
	bloomFilter  *bloom.BloomFilter
	indexOffset  int64
	summaries    int
}

func NewStreamingWriter(opts StreamingWriterOptions) (StreamingWriter, error) {
	w, err := NewWriter(opts.Options)
	if err != nil {
		return nil, err
	}

	writerOpts := DataWriterOpenOptions{
		BlockSize: opts.BlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:   opts.NamespaceID,
			Shard:       opts.ShardID,
			BlockStart:  opts.BlockStart,
			VolumeIndex: opts.VolumeIndex,
		},
		FileSetType: persist.FileSetFlushType,
	}

	m, k := bloom.EstimateFalsePositiveRate(
		opts.PlannedRecordsCount,
		opts.Options.IndexBloomFilterFalsePositivePercent(),
	)
	bloomFilter := bloom.NewBloomFilter(m, k)

	summariesApprox := float64(opts.PlannedRecordsCount) * opts.Options.IndexSummariesPercent()
	summaryEvery := 0
	if summariesApprox > 0 {
		summaryEvery = int(math.Floor(float64(opts.PlannedRecordsCount) / summariesApprox))
	}

	return &streamingWriter{
		opts:         opts,
		writer:       w.(*writer),
		writerOpts:   writerOpts,
		summaryEvery: int64(summaryEvery),
		data:         make([]checked.Bytes, 2),
		bloomFilter:  bloomFilter,
	}, nil
}

func (w *streamingWriter) Open() error {
	if err := w.writer.Open(w.writerOpts); err != nil {
		return err
	}
	w.indexOffset = 0
	w.summaries = 0
	w.prevIDBytes = nil
	return nil
}

func (w *streamingWriter) Write(
	ctx context.Context,
	encoder encoding.Encoder,
	id ident.ID,
	encodedTags []byte,
) error {
	// Need to check if w.prevIDBytes != nil, otherwise we can never write an empty string ID
	if w.prevIDBytes != nil && bytes.Compare(id.Bytes(), w.prevIDBytes) <= 0 {
		return fmt.Errorf("ids must be written in lexicographic order, no duplicates, but got %s followed by %s", w.prevIDBytes, id)
	}
	w.prevIDBytes = append(w.prevIDBytes[:0], id.Bytes()...)

	stream, ok := encoder.Stream(ctx)
	if !ok {
		// None of the datapoints passed the predicate.
		return nil
	}
	defer stream.Finalize()
	segment, err := stream.Segment()
	if err != nil {
		return err
	}
	w.data[0] = segment.Head
	w.data[1] = segment.Tail
	checksum := segment.CalculateChecksum()
	entry, err := w.writeData(w.data, checksum)
	if err != nil {
		return err
	}

	if entry != nil {
		return w.writeIndexRelated(id, encodedTags, entry)
	}

	return nil
}

func (w *streamingWriter) writeData(
	data []checked.Bytes,
	dataChecksum uint32,
) (*indexEntry, error) {
	var size int64
	for _, d := range data {
		if d == nil {
			continue
		}
		size += int64(d.Len())
	}
	if size == 0 {
		return nil, nil
	}

	// Warning: metadata is not set here and should not be used anywhere below.
	entry := &indexEntry{
		index:          w.currIdx,
		dataFileOffset: w.writer.currOffset,
		size:           uint32(size),
		dataChecksum:   dataChecksum,
	}
	for _, d := range data {
		if d == nil {
			continue
		}
		if err := w.writer.writeData(d.Bytes()); err != nil {
			return nil, err
		}
	}

	w.currIdx++

	return entry, nil
}

func (w *streamingWriter) writeIndexRelated(
	id ident.ID,
	encodedTags []byte,
	entry *indexEntry,
) error {
	// Add to the bloom filter, note this must be zero alloc or else this will
	// cause heavy GC churn as we flush millions of series at end of each
	// time window
	w.bloomFilter.Add(id.Bytes())

	if entry.index%w.summaryEvery == 0 {
		// Capture the offset for when we write this summary back, only capture
		// for every summary we'll actually write to avoid a few memcopies
		entry.indexFileOffset = w.indexOffset
	}

	length, err := w.writer.writeIndexWithEncodedTags(id.Bytes(), encodedTags, *entry)
	if err != nil {
		return err
	}
	w.indexOffset += length

	if entry.index%w.summaryEvery == 0 {
		entry.metadata = persist.NewMetadataFromIDAndTagIterator(id, nil, persist.MetadataOptions{})
		err = w.writer.writeSummariesEntry(*entry)
		if err != nil {
			return err
		}
		w.summaries++
	}

	return nil
}

func (w *streamingWriter) Close() error {
	for i := range w.data {
		w.data[i] = nil
	}
	w.prevIDBytes = nil

	// Write the bloom filter bitset out
	if err := w.writer.writeBloomFilterFileContents(w.bloomFilter); err != nil {
		return err
	}

	if err := w.writer.writeInfoFileContents(w.bloomFilter, w.summaries, w.currIdx); err != nil {
		return err
	}

	err := w.writer.closeWOIndex()
	if err != nil {
		w.writer.err = err
		return err
	}

	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := writeCheckpointFile(
		w.writer.checkpointFilePath,
		w.writer.digestFdWithDigestContents.Digest().Sum32(),
		w.writer.digestBuf,
		w.writer.newFileMode,
	); err != nil {
		w.writer.err = err
		return err
	}

	return nil
}
