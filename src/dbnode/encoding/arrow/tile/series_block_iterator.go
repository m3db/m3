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

package tile

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type seriesBlockIter struct {
	reader fs.CrossBlockReader

	err           error
	exhausted     bool
	hasLastValues bool
	concurrency   int
	freedAfter    int

	step  xtime.UnixNano
	start xtime.UnixNano

	encodingOpts encoding.Options
	recorders    []recorder
	iters        []SeriesFrameIterator
	byteReaders  []*bytes.Reader
	baseIters    []encoding.ReaderIterator
	bytesRefHeld []bool
	dataBytes    []checked.Bytes
	tagIters     []ident.TagIterator
	ids          []ident.ID
}

// NewSeriesBlockIterator creates a new SeriesBlockIterator.
func NewSeriesBlockIterator(
	reader fs.CrossBlockReader,
	opts Options,
) (SeriesBlockIterator, error) {
	concurrency := opts.Concurrency
	if concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be greater than 0, is: %d", concurrency)
	}

	var (
		recorders   = make([]recorder, 0, concurrency)
		iters       = make([]SeriesFrameIterator, 0, concurrency)
		byteReaders = make([]*bytes.Reader, 0, concurrency)
		baseIters   = make([]encoding.ReaderIterator, 0, concurrency)
	)

	for i := 0; i < concurrency; i++ {
		var recorder recorder
		if opts.UseArrow {
			recorder = newDatapointRecorder(memory.NewGoAllocator())
		} else {
			recorder = newFlatDatapointRecorder()
		}

		recorders = append(recorders, recorder)
		iters = append(iters, newSeriesFrameIterator(recorder))
		byteReaders = append(byteReaders, bytes.NewReader(nil))
		baseIters = append(baseIters, m3tsz.NewReaderIterator(nil, true, opts.EncodingOpts))
	}

	return &seriesBlockIter{
		reader: reader,

		concurrency: concurrency,
		freedAfter:  concurrency,
		start:       opts.Start,
		step:        opts.FrameSize,

		encodingOpts: opts.EncodingOpts,
		recorders:    recorders,
		iters:        iters,
		byteReaders:  byteReaders,
		baseIters:    baseIters,
		bytesRefHeld: make([]bool, concurrency),
		dataBytes:    make([]checked.Bytes, concurrency),
		tagIters:     make([]ident.TagIterator, concurrency),
		ids:          make([]ident.ID, concurrency),
	}, nil
}

func (b *seriesBlockIter) Next() bool {
	if b.exhausted || b.err != nil {
		return false
	}

	for i, held := range b.bytesRefHeld {
		if held && b.dataBytes[i] != nil {
			b.bytesRefHeld[i] = false
			b.dataBytes[i].DecRef()
		}
	}

	var err error
	for i := 0; i < b.concurrency; i++ {
		if !b.reader.Next() {
			b.exhausted = true
			err = b.reader.Err()
			if err != nil {
				b.err = err
				return false
			}

			// NB: tag iters and data bytes are already released at this index;
			// explicitly free other resources here.
			b.recorders[i].release()
			b.baseIters[i].Close()
			b.byteReaders[i] = nil
			b.err = b.freeAfterIndex(i + 1)
			// NB: if any values remain, provide them to consumer.
			return i > 0
		}

		var blockRecords []fs.BlockRecord
		b.ids[i], b.tagIters[i], blockRecords = b.reader.Current()
		//TODO: currently this reads from the first block only, for each series
		b.dataBytes[i] = blockRecords[0].Data
		for _, blockRecord := range blockRecords[1:] {
			blockRecord.Data.DecRef()
			blockRecord.Data.Finalize()
		}

		b.dataBytes[i].IncRef()
		b.bytesRefHeld[i] = true

		bs := b.dataBytes[i].Bytes()
		b.byteReaders[i].Reset(bs)
		b.baseIters[i].Reset(b.byteReaders[i], nil)
		b.iters[i].Reset(b.start, b.step, b.baseIters[i], b.ids[i], b.tagIters[i])
	}

	return true
}

func (b *seriesBlockIter) freeAfterIndex(fromIdx int) error {
	var multiErr xerrors.MultiError
	for i := fromIdx; i < b.freedAfter; i++ {
		b.recorders[i].release()
		b.baseIters[i].Close()
		b.byteReaders[i] = nil
		if b.tagIters[i] != nil {
			b.tagIters[i].Close()
		}

		if b.ids[i] != nil {
			b.ids[i].Finalize()
		}

		if b.bytesRefHeld[i] {
			b.dataBytes[i].DecRef()
			b.bytesRefHeld[i] = false
		}

		if b.dataBytes[i] != nil {
			b.dataBytes[i].Finalize()
		}

		multiErr = multiErr.Add(b.iters[i].Close())
	}

	b.freedAfter = fromIdx
	return multiErr.LastError()
}

func (b *seriesBlockIter) Current() []SeriesFrameIterator {
	return b.iters
}

func (b *seriesBlockIter) Close() error {
	return b.freeAfterIndex(0)
}

func (b *seriesBlockIter) Err() error {
	return b.err
}
