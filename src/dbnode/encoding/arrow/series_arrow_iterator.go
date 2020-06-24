package arrow

import (
	"time"
)

type arrowSeriesIterator interface {
	next() bool
	close() error
	current() arrowBlockIterator
	reset(iter seriesIterator)
}

type arrowSeriesIter struct {
	step     time.Duration
	recorder *datapointRecorder
	iter     seriesIterator

	exhausted bool
	start     time.Time
	curr      arrowBlockIterator
}

func newArrowSeriesIterator(
	start time.Time,
	step time.Duration,
	recorder *datapointRecorder,
	iter seriesIterator,
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
	if b.iter.next() {
		iter, time := b.iter.current()
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

func (b *arrowSeriesIter) reset(iter seriesIterator) {
	blockIter, start := iter.current()
	b.curr.reset(start, blockIter)
	if b.iter != nil {
		b.iter.close()
	}

	b.iter = iter
}
