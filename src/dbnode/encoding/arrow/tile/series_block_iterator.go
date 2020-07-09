package tile

import (
	"bytes"
	"io"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// SeriesBlockIterator iterates series blocks.
type SeriesBlockIterator interface {
	Next() bool
	Close() error
	Current() SeriesFrameIterator
	Reset(
		start xtime.UnixNano,
		step xtime.UnixNano,
		reader fs.DataFileSetReader,
	) error
	Err() error
}

type seriesBlockIter struct {
	recorder *DatapointRecorder
	reader   fs.DataFileSetReader

	exhausted bool
	step      xtime.UnixNano
	start     xtime.UnixNano
	iters     []SeriesFrameIterator
	err       error
	bytesRef  bool

	iter         SeriesFrameIterator
	byteReader   *bytes.Reader
	baseIter     encoding.ReaderIterator
	dataBytes    checked.Bytes
	encodingOpts encoding.Options
	tagIter      ident.TagIterator
}

// NewSeriesBlockIterator creates a new SeriesBlockIterator.
func NewSeriesBlockIterator(
	recorder *DatapointRecorder,
	reader fs.DataFileSetReader,
	encodingOpts encoding.Options,
	step xtime.UnixNano,
	start xtime.UnixNano,
	concurrency int,
) SeriesBlockIterator {
	return &seriesBlockIter{
		recorder: recorder,
		reader:   reader,

		start: start,
		step:  step,

		iter:         newSeriesFrameIterator(recorder),
		byteReader:   bytes.NewReader(nil),
		baseIter:     m3tsz.NewReaderIterator(nil, true, encodingOpts),
		encodingOpts: encodingOpts,
	}
}

func (b *seriesBlockIter) Next() bool {
	if b.exhausted || b.err != nil {
		return false
	}

	if b.dataBytes != nil && b.bytesRef {
		b.bytesRef = false
		b.dataBytes.DecRef()
	}

	var err error
	_, b.tagIter, b.dataBytes, _, err = b.reader.Read()

	if err != nil {
		b.exhausted = true
		// NB: eof is expected.
		if err != io.EOF {
			b.err = err
		}

		return false
	}

	b.dataBytes.IncRef()
	b.byteReader.Reset(b.dataBytes.Bytes())
	// fmt.Println("       bytes", b.dataBytes.Bytes())
	b.baseIter.Reset(b.byteReader, nil)

	// for b.baseIter.Next() {
	// 	a, _, _ := b.baseIter.Current()
	// 	if a.Value != 0 {
	// 		fmt.Println(a)
	// 	}
	// }
	b.iter.Reset(b.start, b.step, b.baseIter)

	return true
}

func (b *seriesBlockIter) Current() SeriesFrameIterator {
	return b.iter
}

func (b *seriesBlockIter) Reset(
	start xtime.UnixNano,
	step xtime.UnixNano,
	reader fs.DataFileSetReader,
) error {
	if err := reader.Validate(); err != nil {
		return err
	}

	if err := b.iter.Reset(start, step, nil); err != nil {
		return err
	}

	if b.bytesRef {
		b.dataBytes.DecRef()
	}

	b.err = nil
	b.bytesRef = false
	return nil
}

func (b *seriesBlockIter) Close() error {
	b.recorder.release()

	b.baseIter.Close()
	b.byteReader = nil
	b.dataBytes.Finalize()
	b.tagIter.Close()
	return b.iter.Close()
}

func (b *seriesBlockIter) Err() error {
	return b.err
}
