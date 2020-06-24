package arrow

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
)

type arrowSeriesIterator interface {
	next() bool
	close() error
	current() arrowBlockIterator
	reset(iter base.SeriesIterator)
}

type arrowSeriesIter struct {
	step     time.Duration
	recorder *datapointRecorder
	iter     base.SeriesIterator

	exhausted bool
	start     time.Time
	curr      arrowBlockIterator
}

func newArrowSeriesIterator(
	start time.Time,
	step time.Duration,
	recorder *datapointRecorder,
	iter base.SeriesIterator,
) arrowSeriesIterator {
	return &arrowSeriesIter{
		curr:     newArrowBlockIterator(start, step, recorder, nil),
		start:    start,
		step:     step,
		recorder: recorder,
		iter:     iter,
	}
}

func (b *arrowSeriesIter) close() error {
	b.recorder.release()
	return nil
}

func (b *arrowSeriesIter) next() bool {
	if b.iter.Next() {
		iter, time := b.iter.Current()
		b.curr.reset(time, iter)
		return true
	}

	if b.curr != nil {
		b.curr.close()
	}

	return false
}

func (b *arrowSeriesIter) current() arrowBlockIterator {
	return b.curr
}

func (b *arrowSeriesIter) reset(iter base.SeriesIterator) {
	blockIter, start := iter.Current()
	b.curr.reset(start, blockIter)
	if b.iter != nil {
		b.iter.Close()
	}

	b.iter = iter
}
