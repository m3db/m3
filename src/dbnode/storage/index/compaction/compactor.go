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
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/mmap"
)

var (
	errCompactorClosed = errors.New("compactor is closed")
)

// Compactor is a compactor.
type Compactor struct {
	sync.RWMutex

	writer       persist.MutableSegmentFileSetWriter
	docsPool     doc.DocumentArrayPool
	docsMaxBatch int
	fstOpts      fst.Options
	mutableSeg   segment.MutableSegment
	buff         *bytes.Buffer
	closed       bool
}

// NewCompactor returns a new compactor which reuses buffers
// to avoid allocating intermediate buffers when compacting.
func NewCompactor(
	docsPool doc.DocumentArrayPool,
	docsMaxBatch int,
	memOpts mem.Options,
	fstOpts fst.Options,
) (*Compactor, error) {
	writer, err := persist.NewMutableSegmentFileSetWriter()
	if err != nil {
		return nil, err
	}

	mutableSeg, err := mem.NewSegment(0, memOpts)
	if err != nil {
		return nil, err
	}

	return &Compactor{
		writer:       writer,
		docsPool:     docsPool,
		docsMaxBatch: docsMaxBatch,
		mutableSeg:   mutableSeg,
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
	// NB(r): Ensure only single compaction happens at a time since the buffers are
	// reused between runs.
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, errCompactorClosed
	}

	if len(segs) == 1 {
		if seg, ok := segs[0].(segment.MutableSegment); ok {
			// If just a single mutable segment, can compact it directly
			return c.compactMutableSegmentWithLock(seg)
		}
	}

	// Need to combine segments first
	c.mutableSeg.Reset(0)

	batch := c.docsPool.Get()
	defer func() {
		c.docsPool.Put(batch)
	}()

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

			err := c.mutableSeg.InsertBatch(index.Batch{Docs: batch})
			if err != nil {
				return nil, err
			}
			batch = batch[:0]
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

	if len(batch) != 0 {
		// Flush last batch
		err := c.mutableSeg.InsertBatch(index.Batch{Docs: batch})
		if err != nil {
			return nil, err
		}
	}

	return c.compactMutableSegmentWithLock(c.mutableSeg)
}

func (c *Compactor) compactMutableSegmentWithLock(
	seg segment.MutableSegment,
) (segment.Segment, error) {
	// Need to seal first if not already sealed
	if !seg.IsSealed() {
		if _, err := seg.Seal(); err != nil {
			return nil, err
		}
	}

	if err := c.writer.Reset(seg); err != nil {
		return nil, err
	}

	success := false
	fstData := &fstSegmentMetadata{
		major:    c.writer.MajorVersion(),
		minor:    c.writer.MinorVersion(),
		metadata: append([]byte(nil), c.writer.SegmentMetadata()...),
		files:    make([]persist.IndexSegmentFile, 0, len(c.writer.Files())),
	}
	// Cleanup incase we run into issues
	defer func() {
		if !success {
			for _, f := range fstData.files {
				f.Close()
			}
		}
	}()

	for _, f := range c.writer.Files() {
		c.buff.Reset()
		if err := c.writer.WriteFile(f, c.buff); err != nil {
			return nil, err
		}

		fileBytes := c.buff.Bytes()

		// Copy bytes to new mmap region to hide from the GC
		mmapedResult, err := mmap.Bytes(int64(len(fileBytes)), mmap.Options{
			Read:  true,
			Write: true,
		})
		if err != nil {
			return nil, err
		}
		copy(mmapedResult.Result, fileBytes)

		segmentFile := persist.NewMmapedIndexSegmentFile(f, nil, mmapedResult.Result)
		fstData.files = append(fstData.files, segmentFile)
	}

	// NB: need to mark success here as the NewSegment call assumes ownership of
	// the provided bytes regardless of success/failure.
	success = true

	return persist.NewSegment(fstData, c.fstOpts)
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
	mutableSeg := c.mutableSeg
	c.mutableSeg = nil
	c.buff = nil

	return mutableSeg.Close()
}

type fstSegmentMetadata struct {
	major    int
	minor    int
	metadata []byte
	files    []persist.IndexSegmentFile
}

var _ persist.IndexSegmentFileSet = &fstSegmentMetadata{}

func (f *fstSegmentMetadata) SegmentType() persist.IndexSegmentType {
	return persist.FSTIndexSegmentType
}
func (f *fstSegmentMetadata) MajorVersion() int       { return f.major }
func (f *fstSegmentMetadata) MinorVersion() int       { return f.minor }
func (f *fstSegmentMetadata) SegmentMetadata() []byte { return f.metadata }
func (f *fstSegmentMetadata) Files() []persist.IndexSegmentFile {
	return f.files
}
