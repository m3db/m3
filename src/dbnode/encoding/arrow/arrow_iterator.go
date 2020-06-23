package arrow

import "github.com/apache/arrow/go/arrow/array"

type arrowIterator interface {
	next() bool
	Current() array.Record
	// remaining() int
}

type arrowIter struct {
	iter    seriesIterator
	started bool
	current array.Record
	builder *array.RecordBuilder
}

func newArrowIter(iter seriesIterator) arrowIterator {
	return &arrowIter{
		iter: iter,
		// idx:         -1,
		// count:       count,
		// initialTime: initialTime,
	}
}

func (b *arrowIter) next() bool {
	if !b.next() {
		return false
	}

	if !b.started {
		// NB: no need to release first time.
		b.started = true
	} else {
		b.builder.Release()
	}

	b.current = b.builder.NewRecord()
	b.current.Schema().Field(0)

	return true

	// if b.idx >= b.count {
	// 	return false
	// }

	// b.idx++
	// return true
}

// func (b *arrowIter) remaining() int {
// 	return b.blockSize - b.idx + 1
// }

func (b *arrowIter) Current() array.Record {
	return b.current
}
