package tile

import (
	"bytes"
	"fmt"
	"io"

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
	reader fs.DataFileSetReader

	err           error
	exhausted     bool
	hasLastValues bool
	concurrency   int
	freedAfter    int

	step  xtime.UnixNano
	start xtime.UnixNano

	encodingOpts encoding.Options
	recorders    []*datapointRecorder
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
	reader fs.DataFileSetReader,
	step xtime.UnixNano,
	start xtime.UnixNano,
	concurrency int,
	encodingOpts encoding.Options,
) (SeriesBlockIterator, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be greater than 0, is: %d", concurrency)
	}

	var (
		recorders   = make([]*datapointRecorder, 0, concurrency)
		iters       = make([]SeriesFrameIterator, 0, concurrency)
		byteReaders = make([]*bytes.Reader, 0, concurrency)
		baseIters   = make([]encoding.ReaderIterator, 0, concurrency)
	)

	for i := 0; i < concurrency; i++ {
		recorder := newDatapointRecorder(memory.NewGoAllocator())
		recorders = append(recorders, recorder)
		iters = append(iters, newSeriesFrameIterator(recorder))
		byteReaders = append(byteReaders, bytes.NewReader(nil))
		baseIters = append(baseIters, m3tsz.NewReaderIterator(nil, true, encodingOpts))
	}

	return &seriesBlockIter{
		reader: reader,

		concurrency: concurrency,
		freedAfter:  concurrency,
		start:       start,
		step:        step,

		encodingOpts: encodingOpts,
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
		b.ids[i], b.tagIters[i], b.dataBytes[i], _, err = b.reader.Read()

		if err != nil {
			b.exhausted = true
			// NB: errors other than EOF should halt execution.
			if err != io.EOF {
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

		b.dataBytes[i].IncRef()
		b.bytesRefHeld[i] = true

		bs := b.dataBytes[i].Bytes()
		// bbs := append(make([]byte, 0, len(bs)), bs...)
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
		b.tagIters[i].Close()
		b.ids[i].Finalize()
		if b.bytesRefHeld[i] {
			b.dataBytes[i].DecRef()
			b.bytesRefHeld[i] = false
		}
		b.dataBytes[i].Finalize()
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
