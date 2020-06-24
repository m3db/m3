package base

import (
	"time"
)

// SeriesIterator is an iterator across multiple series blocks.
type SeriesIterator interface {
	// Next moves to the next element in the iterator.
	Next() bool
	// Current yields the current value.
	Current() (BlockIterator, time.Time)
	// Remaining reports how many elements are remaining.
	Remaining() int
	// Close closes the iterator.
	Close() error
}

type seriesIter struct {
	initialTime int64
	blockSize   int
	stepSize    int
	idx         int
	count       int
}

// NewSeriesIterator creates a simple iterator across multiple series blocks.
func NewSeriesIterator(
	count int, initialTime int64, stepSize int, blockSize int,
) SeriesIterator {
	return &seriesIter{
		idx:         -1,
		count:       count - 1,
		stepSize:    stepSize,
		blockSize:   blockSize,
		initialTime: initialTime,
	}
}

func (b *seriesIter) Next() bool {
	if b.idx >= b.count {
		return false
	}

	b.idx++
	return true
}

func (b *seriesIter) Remaining() int {
	return b.count - b.idx
}

func (b *seriesIter) Current() (BlockIterator, time.Time) {
	init := b.initialTime + int64(b.idx*b.blockSize)
	return NewBlockIterator(init, b.stepSize, b.blockSize), time.Unix(0, init)
}

func (b *seriesIter) Close() error { return nil }
