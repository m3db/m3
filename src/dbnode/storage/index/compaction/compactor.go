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
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/mmap"
	xerrors "github.com/m3db/m3x/errors"
)

var (
	errCompactorBuilderEmpty = errors.New("builder has no documents")
	errCompactorClosed       = errors.New("compactor is closed")
)

// Compactor is a compactor.
type Compactor struct {
	sync.RWMutex

	writer       fst.Writer
	docsPool     doc.DocumentArrayPool
	docsMaxBatch int
	fstOpts      fst.Options
	builder      segment.SegmentsBuilder
	buff         *bytes.Buffer
	closed       bool
}

// NewCompactor returns a new compactor which reuses buffers
// to avoid allocating intermediate buffers when compacting.
func NewCompactor(
	docsPool doc.DocumentArrayPool,
	docsMaxBatch int,
	builderOpts builder.Options,
	fstOpts fst.Options,
) (*Compactor, error) {
	builder, err := builder.NewBuilderFromSegments(builderOpts)
	if err != nil {
		return nil, err
	}
	return &Compactor{
		writer:       fst.NewWriter(),
		docsPool:     docsPool,
		docsMaxBatch: docsMaxBatch,
		builder:      builder,
		fstOpts:      fstOpts,
		buff:         bytes.NewBuffer(nil),
	}, nil
}

// Compact will take a set of segments and compact them into an immutable
// FST segment, if there is a single mutable segment it can directly be
// converted into an FST segment, otherwise an intermediary mutable segment
// (reused by the compactor between runs) is used to combine all the segments
// together first before compacting into an FST segment.
// Note: This is thread safe and only a single compaction may happen at a time.
func (c *Compactor) Compact(segs []segment.Segment) (segment.Segment, error) {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, errCompactorClosed
	}

	c.builder.Reset(0)
	if err := c.builder.AddSegments(segs); err != nil {
		return nil, err
	}

	return c.compactFromBuilderWithLock(c.builder)
}

// CompactUsingBuilder compacts segments together using a provided segment builder.
func (c *Compactor) CompactUsingBuilder(
	builder segment.DocumentsBuilder,
	segs []segment.Segment,
) (segment.Segment, error) {
	// NB(r): Ensure only single compaction happens at a time since the buffers are
	// reused between runs.
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, errCompactorClosed
	}

	if builder == nil {
		return nil, errCompactorBuilderEmpty
	}

	if len(segs) == 0 {
		// No segments to compact, just compact from the builder
		return c.compactFromBuilderWithLock(builder)
	}

	// Need to combine segments first
	batch := c.docsPool.Get()
	defer func() {
		c.docsPool.Put(batch)
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

	return c.compactFromBuilderWithLock(builder)
}

func (c *Compactor) compactFromBuilderWithLock(
	builder segment.Builder,
) (segment.Segment, error) {
	defer func() {
		// Release resources regardless of result,
		// otherwise old compacted segments are held onto
		// strongly
		builder.Reset(0)
	}()

	// Since this builder is likely reused between compaction
	// runs, we need to copy the docs slice
	allDocs := builder.Docs()
	if len(allDocs) == 0 {
		return nil, errCompactorBuilderEmpty
	}

	allDocsCopy := make([]doc.Document, len(allDocs))
	copy(allDocsCopy, allDocs)

	err := c.writer.Reset(builder)
	if err != nil {
		return nil, err
	}

	success := false
	closers := new(closers)
	fstData := fst.SegmentData{
		MajorVersion: c.writer.MajorVersion(),
		MinorVersion: c.writer.MinorVersion(),
		Metadata:     append([]byte(nil), c.writer.Metadata()...),
		DocsReader:   docs.NewSliceReader(0, allDocsCopy),
		Closer:       closers,
	}

	// Cleanup incase we run into issues
	defer func() {
		if !success {
			closers.Close()
		}
	}()

	c.buff.Reset()
	if err := c.writer.WritePostingsOffsets(c.buff); err != nil {
		return nil, err
	}

	fstData.PostingsData, err = c.mmapAndAppendCloser(c.buff.Bytes(), closers)
	if err != nil {
		return nil, err
	}

	c.buff.Reset()
	if err := c.writer.WriteFSTTerms(c.buff); err != nil {
		return nil, err
	}

	fstData.FSTTermsData, err = c.mmapAndAppendCloser(c.buff.Bytes(), closers)
	if err != nil {
		return nil, err
	}

	c.buff.Reset()
	if err := c.writer.WriteFSTFields(c.buff); err != nil {
		return nil, err
	}

	fstData.FSTFieldsData, err = c.mmapAndAppendCloser(c.buff.Bytes(), closers)
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

func (c *Compactor) mmapAndAppendCloser(
	fromBytes []byte,
	closers *closers,
) ([]byte, error) {
	// Copy bytes to new mmap region to hide from the GC
	mmapedResult, err := mmap.Bytes(int64(len(fromBytes)), mmap.Options{
		Read:  true,
		Write: true,
	})
	if err != nil {
		return nil, err
	}
	copy(mmapedResult.Result, fromBytes)

	closers.Append(closer(func() error {
		return mmap.Munmap(mmapedResult.Result)
	}))

	return mmapedResult.Result, nil
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
	c.docsPool = nil
	c.fstOpts = nil
	c.builder = nil
	c.buff = nil

	return nil
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
