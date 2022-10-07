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

package compaction

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/mmap"

	"github.com/uber-go/tally"
)

var (
	// ErrCompactorBuilderEmpty is returned when the compaction
	// would result in an empty segment.
	ErrCompactorBuilderEmpty = errors.New("builder has no documents")
	errCompactorBuilderNil   = errors.New("builder is nil")
	errCompactorClosed       = errors.New("compactor is closed")
)

const (
	fileModeScratchMinSize = 2 << 14 // 32k docs
)

// Compactor is a compactor.
type Compactor struct {
	sync.RWMutex

	opts         CompactorOptions
	writer       fst.Writer
	metadataPool doc.MetadataArrayPool
	docsMaxBatch int
	fstOpts      fst.Options
	builder      segment.SegmentsBuilder
	scratch      *bufferOrTempFile
	closed       bool

	metrics compactorMetrics
}

type compactorMetrics struct {
	compactMemScratchBytes  tally.Counter
	compactFileScratchBytes tally.Counter
}

var _ io.Writer = (*bufferOrTempFile)(nil)

type bufferOrTempFile struct {
	fileMode bool
	buff     *bytes.Buffer

	tempFileBuffer  *bufio.Writer
	tempFile        *os.File
	tempFileWritten int64

	stats scratchStats
}

type scratchStats struct {
	writeTotal int64
}

func newBufferOrTempFile() (*bufferOrTempFile, error) {
	tempFile, err := os.CreateTemp(os.TempDir(), "m3db_compact_scratch*")
	if err != nil {
		return nil, err
	}
	return &bufferOrTempFile{
		buff:           bytes.NewBuffer(nil),
		tempFileBuffer: bufio.NewWriterSize(tempFile, 1<<22 /* 4mb */),
		tempFile:       tempFile,
	}, nil
}

func (b *bufferOrTempFile) EnableFileMode() {
	b.fileMode = true
}

func (b *bufferOrTempFile) DisableFileMode() {
	b.fileMode = false
}

func (b *bufferOrTempFile) Reset() error {
	if b.fileMode {
		if err := b.tempFile.Truncate(0); err != nil {
			return err
		}
		if _, err := b.tempFile.Seek(0, 0); err != nil {
			return err
		}
		b.stats.writeTotal += b.tempFileWritten
		b.tempFileWritten = 0
	} else {
		b.stats.writeTotal += int64(b.buff.Len())
		b.buff.Reset()
	}
	return nil
}

func (b *bufferOrTempFile) Stats() scratchStats {
	return b.stats
}

func (b *bufferOrTempFile) ResetStats() {
	b.stats = scratchStats{}
}

func (b *bufferOrTempFile) Write(p []byte) (int, error) {
	if b.fileMode {
		n, err := b.tempFileBuffer.Write(p)
		if err != nil {
			return 0, err
		}
		b.tempFileWritten += int64(n)
		return n, nil
	}
	return b.buff.Write(p)
}

func (b *bufferOrTempFile) Reader() (io.Reader, error) {
	if b.fileMode {
		if err := b.tempFileBuffer.Flush(); err != nil {
			return nil, err
		}
		if _, err := b.tempFile.Seek(0, 0); err != nil {
			return nil, err
		}
		return b.tempFile, nil
	}
	return b.buff, nil
}

func (b *bufferOrTempFile) Size() int64 {
	if b.fileMode {
		return b.tempFileWritten
	}
	return int64(b.buff.Len())
}

func (b *bufferOrTempFile) Close() error {
	file := b.tempFile.Name()
	if err := b.tempFile.Close(); err != nil {
		return err
	}
	return os.Remove(file)
}

// CompactorOptions is a set of compactor options.
type CompactorOptions struct {
	// FSTWriterOptions if not nil are the options used to
	// construct the FST writer.
	FSTWriterOptions *fst.WriterOptions

	// MmapDocsData when enabled will encode and mmmap the
	// documents data, rather than keeping the original
	// documents with references to substrings in the metric
	// IDs (done for memory savings).
	MmapDocsData bool
}

// NewCompactor returns a new compactor which reuses buffers
// to avoid allocating intermediate buffers when compacting.
func NewCompactor(
	metadataPool doc.MetadataArrayPool,
	docsMaxBatch int,
	builderOpts builder.Options,
	fstOpts fst.Options,
	opts CompactorOptions,
) (*Compactor, error) {
	var fstWriterOpts fst.WriterOptions
	if v := opts.FSTWriterOptions; v != nil {
		fstWriterOpts = *v
	}
	writer, err := fst.NewWriter(fstWriterOpts)
	if err != nil {
		return nil, err
	}
	scratch, err := newBufferOrTempFile()
	if err != nil {
		return nil, err
	}
	const (
		indexCompactor           = "index_compactor"
		compactWriteType         = "type"
		compactWriteScratchBytes = "compact_write_scratch_bytes"
	)
	scope := fstOpts.InstrumentOptions().MetricsScope().SubScope(indexCompactor)
	return &Compactor{
		opts:         opts,
		writer:       writer,
		metadataPool: metadataPool,
		docsMaxBatch: docsMaxBatch,
		builder:      builder.NewBuilderFromSegments(builderOpts),
		fstOpts:      fstOpts,
		scratch:      scratch,
		metrics: compactorMetrics{
			compactMemScratchBytes: scope.Tagged(map[string]string{
				compactWriteType: "mem",
			}).Counter(compactWriteScratchBytes),
			compactFileScratchBytes: scope.Tagged(map[string]string{
				compactWriteType: "file",
			}).Counter(compactWriteScratchBytes),
		},
	}, nil
}

// CompactResult is the result of a call to compact.
type CompactResult struct {
	Compacted        fst.Segment
	SegmentMetadatas []segment.SegmentsBuilderSegmentMetadata
}

// Compact will take a set of segments and compact them into an immutable
// FST segment, if there is a single mutable segment it can directly be
// converted into an FST segment, otherwise an intermediary mutable segment
// (reused by the compactor between runs) is used to combine all the segments
// together first before compacting into an FST segment.
// Note: This is not thread safe and only a single compaction may happen at a
// time.
func (c *Compactor) Compact(
	segs []segment.Segment,
	filter segment.DocumentsFilter,
	filterCounter tally.Counter,
	reporterOptions mmap.ReporterOptions,
) (CompactResult, error) {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return CompactResult{}, errCompactorClosed
	}

	c.builder.Reset()
	c.builder.SetFilter(filter, filterCounter)
	if err := c.builder.AddSegments(segs); err != nil {
		return CompactResult{}, err
	}

	metas, err := c.builder.SegmentMetadatas()
	if err != nil {
		return CompactResult{}, err
	}

	var size int64
	for _, seg := range segs {
		size += seg.Size()
	}
	compacted, err := c.compactFromBuilderWithLock(c.builder, size, reporterOptions)
	if err != nil {
		return CompactResult{}, err
	}

	return CompactResult{
		Compacted:        compacted,
		SegmentMetadatas: metas,
	}, nil
}

// CompactUsingBuilder compacts segments together using a provided segment builder.
func (c *Compactor) CompactUsingBuilder(
	builder segment.DocumentsBuilder,
	segs []segment.Segment,
	reporterOptions mmap.ReporterOptions,
) (fst.Segment, error) {
	// NB(r): Ensure only single compaction happens at a time since the buffers are
	// reused between runs.
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, errCompactorClosed
	}

	if builder == nil {
		return nil, errCompactorBuilderNil
	}

	if len(segs) == 0 {
		// No segments to compact, just compact from the builder
		size := int64(len(builder.Docs()))
		return c.compactFromBuilderWithLock(builder, size, reporterOptions)
	}

	// Need to combine segments first
	batch := c.metadataPool.Get()
	defer func() {
		c.metadataPool.Put(batch)
	}()

	// flushBatch is declared to reuse the same code from the
	// inner loop and the completion of the loop
	flushBatch := func() error {
		if len(batch) == 0 {
			// Last flush might not have any docs enqueued
			return nil
		}

		err := builder.InsertBatch(index.Batch{
			Docs:                batch,
			AllowPartialUpdates: true,
		})
		if err != nil && index.IsBatchPartialError(err) {
			// If after filtering out duplicate ID errors
			// there are no errors, then this was a successful
			// insertion.
			batchErr := err.(*index.BatchPartialError)
			// NB(r): FilterDuplicateIDErrors returns nil
			// if no errors remain after filtering duplicate ID
			// errors, this case is covered in unit tests.
			err = batchErr.FilterDuplicateIDErrors()
		}
		if err != nil {
			return err
		}

		// Reset docs batch for reuse
		var empty doc.Metadata
		for i := range batch {
			batch[i] = empty
		}
		batch = batch[:0]
		return nil
	}

	for _, seg := range segs {
		reader, err := seg.Reader()
		if err != nil {
			return nil, err
		}

		iter, err := reader.AllDocs()
		if err != nil {
			return nil, err
		}

		for iter.Next() {
			batch = append(batch, iter.Current())
			if len(batch) < c.docsMaxBatch {
				continue
			}
			if err := flushBatch(); err != nil {
				return nil, err
			}
		}

		if err := iter.Err(); err != nil {
			return nil, err
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}

	// Last flush in case some remaining docs that
	// weren't written to the mutable segment
	if err := flushBatch(); err != nil {
		return nil, err
	}

	size := int64(len(builder.Docs()))
	return c.compactFromBuilderWithLock(builder, size, reporterOptions)
}

func (c *Compactor) compactFromBuilderWithLock(
	builder segment.Builder,
	size int64,
	reporterOptions mmap.ReporterOptions,
) (fst.Segment, error) {
	// Use file mode just whenever big enough.
	if size > fileModeScratchMinSize {
		c.scratch.EnableFileMode()
	} else {
		c.scratch.DisableFileMode()
	}

	// Make sure stats start from beginning.
	c.scratch.ResetStats()

	defer func() {
		// Release resources regardless of result,
		// otherwise old compacted segments are held onto
		// strongly
		builder.Reset()

		// Reset the scratch (important to do before inc metrics since stats are saved when reset called).
		c.scratch.Reset()

		// Record scratch stats.
		if c.scratch.fileMode {
			c.metrics.compactFileScratchBytes.Inc(c.scratch.Stats().writeTotal)
		} else {
			c.metrics.compactMemScratchBytes.Inc(c.scratch.Stats().writeTotal)
		}
	}()

	// Since this builder is likely reused between compaction
	// runs, we need to copy the docs slice
	allDocs := builder.Docs()
	if len(allDocs) == 0 {
		return nil, ErrCompactorBuilderEmpty
	}

	err := c.writer.Reset(builder)
	if err != nil {
		return nil, err
	}

	success := false
	closers := new(closers)
	fstData := fst.SegmentData{
		Version: fst.Version{
			Major: c.writer.MajorVersion(),
			Minor: c.writer.MinorVersion(),
		},
		Metadata: append([]byte(nil), c.writer.Metadata()...),
		Closer:   closers,
	}

	// Cleanup incase we run into issues
	defer func() {
		if !success {
			closers.Close()
		}
	}()

	if !c.opts.MmapDocsData {
		// If retaining references to the original docs, simply take ownership
		// of the documents and then reference them directly from the FST segment
		// rather than encoding them and mmap'ing the encoded documents.
		allDocsCopy := make([]doc.Metadata, len(allDocs))
		copy(allDocsCopy, allDocs)
		fstData.DocsReader = docs.NewSliceReader(allDocsCopy)
	} else {
		// Otherwise encode and reference the encoded bytes as mmap'd bytes.
		if err := c.scratch.Reset(); err != nil {
			return nil, err
		}
		if err := c.writer.WriteDocumentsData(c.scratch); err != nil {
			return nil, err
		}

		fstData.DocsData, err = c.mmapScratchAndAppendCloser(closers, reporterOptions)
		if err != nil {
			return nil, err
		}

		if err := c.scratch.Reset(); err != nil {
			return nil, err
		}
		if err := c.writer.WriteDocumentsIndex(c.scratch); err != nil {
			return nil, err
		}

		fstData.DocsIdxData, err = c.mmapScratchAndAppendCloser(closers, reporterOptions)
		if err != nil {
			return nil, err
		}
	}

	if err := c.scratch.Reset(); err != nil {
		return nil, err
	}
	if err := c.writer.WritePostingsOffsets(c.scratch); err != nil {
		return nil, err
	}

	fstData.PostingsData, err = c.mmapScratchAndAppendCloser(closers, reporterOptions)
	if err != nil {
		return nil, err
	}

	if err := c.scratch.Reset(); err != nil {
		return nil, err
	}
	if err := c.writer.WriteFSTTerms(c.scratch); err != nil {
		return nil, err
	}

	fstData.FSTTermsData, err = c.mmapScratchAndAppendCloser(closers, reporterOptions)
	if err != nil {
		return nil, err
	}

	if err := c.scratch.Reset(); err != nil {
		return nil, err
	}
	if err := c.writer.WriteFSTFields(c.scratch); err != nil {
		return nil, err
	}

	fstData.FSTFieldsData, err = c.mmapScratchAndAppendCloser(closers, reporterOptions)
	if err != nil {
		return nil, err
	}

	compacted, err := fst.NewSegment(fstData, c.fstOpts)
	if err != nil {
		return nil, err
	}

	success = true

	return compacted, nil
}

func (c *Compactor) mmapScratchAndAppendCloser(
	closers *closers,
	reporterOptions mmap.ReporterOptions,
) (mmap.Descriptor, error) {
	scratchReader, err := c.scratch.Reader()
	if err != nil {
		return mmap.Descriptor{}, err
	}

	// Copy bytes to new mmap region to hide from the GC
	mmapedResult, err := mmap.Bytes(c.scratch.Size(), mmap.Options{
		Read:            true,
		Write:           true,
		ReporterOptions: reporterOptions,
	})
	if err != nil {
		return mmap.Descriptor{}, err
	}

	if _, err := io.ReadFull(scratchReader, mmapedResult.Bytes); err != nil {
		_ = mmap.Munmap(mmapedResult)
		return mmap.Descriptor{}, err
	}

	closers.Append(closer(func() error {
		return mmap.Munmap(mmapedResult)
	}))

	return mmapedResult, nil
}

// Close closes the compactor and frees buffered resources.
func (c *Compactor) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return errCompactorClosed
	}

	c.closed = true

	c.writer = nil
	c.metadataPool = nil
	c.fstOpts = nil
	c.builder = nil
	closeErr := c.scratch.Close()
	c.scratch = nil

	return closeErr
}

var _ io.Closer = closer(nil)

type closer func() error

func (c closer) Close() error {
	return c()
}

var _ io.Closer = &closers{}

type closers struct {
	closers []io.Closer
}

func (c *closers) Append(elem io.Closer) {
	c.closers = append(c.closers, elem)
}

func (c *closers) Close() error {
	multiErr := xerrors.NewMultiError()
	for _, elem := range c.closers {
		multiErr = multiErr.Add(elem.Close())
	}
	return multiErr.FinalError()
}
