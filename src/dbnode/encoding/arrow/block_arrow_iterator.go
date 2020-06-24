package arrow

import (
	"time"
)

type arrowBlockIterator interface {
	next() bool
	close() error
	current() datapointRecord
	reset(start time.Time, iter blockIterator)
}

type arrowBlockIter struct {
	step     time.Duration
	recorder *datapointRecorder
	iter     blockIterator

	exhausted bool
	start     time.Time
	curr      datapointRecord
}

func newArrowBlockIterator(
	start time.Time,
	step time.Duration,
	recorder *datapointRecorder,
	iter blockIterator,
) arrowBlockIterator {
	return &arrowBlockIter{
		start:    start,
		step:     step,
		recorder: recorder,
		iter:     iter,
	}
}

func (b *arrowBlockIter) reset(start time.Time, iter blockIterator) {
	if b.iter != nil {
		b.iter.close()
	}

	if b.curr.Record != nil {
		b.curr.Release()
	}

	b.curr.Record = nil
	b.iter = iter
	b.start = start
	b.exhausted = false
}

func (b *arrowBlockIter) close() error {
	if b.iter != nil {
		b.iter.close()
	}

	return nil
}

func (b *arrowBlockIter) Release() {
	b.curr.Release()
}

func (b *arrowBlockIter) next() bool {
	if b.curr.Record != nil {
		b.curr.Release()
	} else {
		// NB: initialize iterator to valid value to start.
		if !b.iter.next() {
			return false
		}
	}

	if b.exhausted {
		return false
	}

	b.start = b.start.Add(b.step)
	cutover := b.start.UnixNano()

	firstPoint := b.iter.current()
	if firstPoint.ts > cutover {
		// NB: empty block.
		return true
	}

	var hasMore bool
	b.recorder.appendPoints(firstPoint)
	for b.iter.next() {
		curr := b.iter.current()
		if curr.ts >= cutover {
			hasMore = true
			break
		}

		b.recorder.appendPoints(curr)
	}

	b.curr = b.recorder.buildRecord()
	if !hasMore {
		b.exhausted = true
	}

	return true
}

func (b *arrowBlockIter) current() datapointRecord {
	return b.curr
}
