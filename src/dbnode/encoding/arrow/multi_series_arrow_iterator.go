package arrow

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
)

type arrowMultiSeriesIterator interface {
	next() bool
	close() error
	current() arrowSeriesIterator
}

type arrowMultiSeriesIter struct {
	step     time.Duration
	recorder *datapointRecorder
	iter     base.MultiSeriesIterator

	exhausted bool
	start     time.Time
	curr      arrowSeriesIterator
}

func newArrowMultiSeriesIterator(
	start time.Time,
	step time.Duration,
	recorder *datapointRecorder,
	iter base.MultiSeriesIterator,
) arrowMultiSeriesIterator {
	return &arrowMultiSeriesIter{
		curr:     newArrowSeriesIterator(start, step, recorder, nil),
		start:    start,
		step:     step,
		recorder: recorder,
		iter:     iter,
	}
}

func (b *arrowMultiSeriesIter) close() error {
	b.recorder.release()
	return nil
}

func (b *arrowMultiSeriesIter) next() bool {
	if b.iter.Next() {
		iter := b.iter.Current()
		b.curr.reset(iter)
		return true
	}

	b.curr.close()
	b.curr = nil
	return false
}

func (b *arrowMultiSeriesIter) current() arrowSeriesIterator {
	return b.curr
}
