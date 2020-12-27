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
	"io"
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"

	"github.com/m3db/bloom/v4"
)

// StreamingWriter writes into data fileset without intermediate buffering.
// Writes must be lexicographically ordered by the id.
type StreamingWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard in the given namespace.
	Open(opts StreamingWriterOpenOptions) error

	// WriteAll will write the id and all byte slices and returns an error on a write error.
	// Callers should call this method with strictly lexicographically increasing ID values.
	WriteAll(id ident.BytesID, encodedTags ts.EncodedTags, data [][]byte, dataChecksum uint32) error

	// Abort closes the file descriptors without writing out a checkpoint file.
	Abort() error
}

// StreamingWriterOpenOptions in the options for the StreamingWriter.
type StreamingWriterOpenOptions struct {
	NamespaceID ident.ID
	ShardID     uint32
	BlockStart  time.Time
	BlockSize   time.Duration
	VolumeIndex int

	// PlannedRecordsCount is an estimate of the number of series to be written.
	// Must be greater than 0.
	PlannedRecordsCount uint
}

type streamingWriter struct {
	writer       *writer
	options      Options
	currIdx      int64
	prevIDBytes  []byte
	summaryEvery int64
	bloomFilter  *bloom.BloomFilter
	indexOffset  int64
	summaries    int
}

// NewStreamingWriter creates a new streaming writer that writes into the data
// fileset without buffering.
func NewStreamingWriter(opts Options) (StreamingWriter, error) {
	w, err := NewWriter(opts)
	if err != nil {
		return nil, err
	}

	return &streamingWriter{writer: w.(*writer), options: opts}, nil
}

func (w *streamingWriter) Open(opts StreamingWriterOpenOptions) error {
	if opts.PlannedRecordsCount <= 0 {
		return fmt.Errorf(
			"PlannedRecordsCount must be positive, got %d", opts.PlannedRecordsCount)
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

	plannedRecordsCount := opts.PlannedRecordsCount
	if plannedRecordsCount == 0 {
		plannedRecordsCount = 1
	}
	m, k := bloom.EstimateFalsePositiveRate(
		plannedRecordsCount,
		w.options.IndexBloomFilterFalsePositivePercent(),
	)
	w.bloomFilter = bloom.NewBloomFilter(m, k)

	summariesApprox := float64(opts.PlannedRecordsCount) * w.options.IndexSummariesPercent()
	w.summaryEvery = 1
	if summariesApprox > 0 {
		w.summaryEvery = int64(math.Max(1,
			math.Floor(float64(opts.PlannedRecordsCount)/summariesApprox)))
	}

	if err := w.writer.Open(writerOpts); err != nil {
		return err
	}
	w.indexOffset = 0
	w.summaries = 0
	w.prevIDBytes = nil
	return nil
}

func (w *streamingWriter) WriteAll(
	id ident.BytesID,
	encodedTags ts.EncodedTags,
	data [][]byte,
	dataChecksum uint32,
) error {
	// Need to check if w.prevIDBytes != nil, otherwise we can never write an empty string ID
	if w.prevIDBytes != nil && bytes.Compare(id, w.prevIDBytes) <= 0 {
		return fmt.Errorf("ids must be written in lexicographic order, no duplicates, but got %s followed by %s", w.prevIDBytes, id)
	}
	w.prevIDBytes = append(w.prevIDBytes[:0], id...)

	entry, ok, err := w.writeData(data, dataChecksum)
	if err != nil {
		return err
	}

	if ok {
		return w.writeIndexRelated(id, encodedTags, entry)
	}

	return nil
}

func (w *streamingWriter) writeData(
	data [][]byte,
	dataChecksum uint32,
) (indexEntry, bool, error) {
	var size int64
	for _, d := range data {
		size += int64(len(d))
	}
	if size == 0 {
		return indexEntry{}, false, nil
	}

	entry := indexEntry{
		index:          w.currIdx,
		dataFileOffset: w.writer.currOffset,
		size:           uint32(size),
		dataChecksum:   dataChecksum,
	}
	for _, d := range data {
		if err := w.writer.writeData(d); err != nil {
			return indexEntry{}, false, err
		}
	}

	w.currIdx++

	return entry, true, nil
}

func (w *streamingWriter) writeIndexRelated(
	id ident.BytesID,
	encodedTags ts.EncodedTags,
	entry indexEntry,
) error {
	// Add to the bloom filter, note this must be zero alloc or else this will
	// cause heavy GC churn as we flush millions of series at end of each
	// time window
	w.bloomFilter.Add(id)

	writeSummary := w.summaryEvery == 0 || entry.index%w.summaryEvery == 0
	if writeSummary {
		// Capture the offset for when we write this summary back, only capture
		// for every summary we'll actually write to avoid a few memcopies
		entry.indexFileOffset = w.indexOffset
	}

	length, err := w.writer.writeIndexWithEncodedTags(id, encodedTags, entry)
	if err != nil {
		return err
	}
	w.indexOffset += length

	if writeSummary {
		err = w.writer.writeSummariesEntry(id, entry)
		if err != nil {
			return err
		}
		w.summaries++
	}

	return nil
}

func (w *streamingWriter) Close() error {
	// Write the bloom filter bitset out
	if err := w.writer.writeBloomFilterFileContents(w.bloomFilter); err != nil {
		return err
	}

	if err := w.writer.writeInfoFileContents(w.bloomFilter, w.summaries, w.currIdx); err != nil {
		return err
	}

	w.bloomFilter = nil

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

func (w *streamingWriter) Abort() error {
	err := w.writer.closeWOIndex()
	if err != nil {
		w.writer.err = err
		return err
	}

	return nil
}
