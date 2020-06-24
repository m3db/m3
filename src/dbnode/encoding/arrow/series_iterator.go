package arrow

import (
	"time"
)

type seriesIterator interface {
	next() bool
	current() (blockIterator, time.Time)
	remaining() int
	close() error
}

type seriesIter struct {
	initialTime int64
	blockSize   int
	stepSize    int
	idx         int
	count       int
}

func newSeriesIterator(
	count int, initialTime int64, stepSize int, blockSize int,
) seriesIterator {
	return &seriesIter{
		idx:         -1,
		count:       count - 1,
		stepSize:    stepSize,
		blockSize:   blockSize,
		initialTime: initialTime,
	}
}

func (b *seriesIter) next() bool {
	if b.idx >= b.count {
		return false
	}

	b.idx++
	return true
}

func (b *seriesIter) remaining() int {
	return b.count - b.idx
}

func (b *seriesIter) current() (blockIterator, time.Time) {
	init := b.initialTime + int64(b.idx*b.blockSize)
	return newBlockIterator(init, b.stepSize, b.blockSize), time.Unix(0, init)
}

func (b *seriesIter) close() error { return nil }
