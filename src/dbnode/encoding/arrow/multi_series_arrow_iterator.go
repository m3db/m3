package arrow

import (
	"time"
)

type arrowMultiSeriesIterator interface {
	next() bool
	close() error
	current() arrowSeriesIterator
}

type arrowMultiSeriesIter struct {
	step     time.Duration
	recorder *datapointRecorder
	iter     multiSeriesIterator

	exhausted bool
	start     time.Time
	curr      arrowSeriesIterator
}

func newArrowMultiSeriesIterator(
	start time.Time,
	step time.Duration,
	recorder *datapointRecorder,
	iter multiSeriesIterator,
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
	if b.iter.next() {
		iter := b.iter.current()
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
