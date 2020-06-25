package tile

import (
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
)

type SeriesBlockIterator interface {
	Next() bool
	Close() error
	Current() SeriesFrameIterator
	Reset(iter base.SeriesIterator)
}

type seriesBlockIter struct {
	recorder *datapointRecorder
	iter     base.SeriesIterator

	exhausted bool
	step      xtime.UnixNano
	start     xtime.UnixNano
	curr      SeriesFrameIterator
}

func newSeriesBlockIterator(
	start xtime.UnixNano,
	step xtime.UnixNano,
	recorder *datapointRecorder,
	iter base.SeriesIterator,
) SeriesBlockIterator {
	return &seriesBlockIter{
		curr:     newSeriesFrameIterator(start, step, recorder, nil),
		start:    start,
		step:     step,
		recorder: recorder,
		iter:     iter,
	}
}

func (b *seriesBlockIter) Close() error {
	b.recorder.release()
	return nil
}

func (b *seriesBlockIter) Next() bool {
	if b.iter.Next() {
		iter, time := b.iter.Current()
		b.curr.Reset(time, iter)
		return true
	}

	if b.curr != nil {
		b.curr.Close()
	}

	return false
}

func (b *seriesBlockIter) Current() SeriesFrameIterator {
	return b.curr
}

func (b *seriesBlockIter) Reset(iter base.SeriesIterator) {
	blockIter, start := iter.Current()
	b.curr.Reset(start, blockIter)
	if b.iter != nil {
		b.iter.Close()
	}

	b.iter = iter
}
