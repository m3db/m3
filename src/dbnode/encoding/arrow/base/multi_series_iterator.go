package base

// MultiSeriesIterator is an iterator across multiple series,
// each containing multiple blocks.
type MultiSeriesIterator interface {
	// Next moves to the next element in the iterator.
	Next() bool
	// Current yields the current value.
	Current() SeriesIterator
	// Remaining reports how many elements are remaining.
	Remaining() int
}

type multiSeriesIter struct {
	initialTime int64
	blockSize   int
	seriesCount int
	stepSize    int
	idx         int
	count       int
}

// NewMultiSeriesIterator creates a new iterator that spans multiple
// test series, each containing a number of contiguous blocks.
func NewMultiSeriesIterator(
	seriesCount int, count int, initialTime int64, stepSize int, blockSize int,
) MultiSeriesIterator {
	return &multiSeriesIter{
		idx:         -1,
		count:       count,
		stepSize:    stepSize,
		blockSize:   blockSize,
		seriesCount: seriesCount - 1,
		initialTime: initialTime,
	}
}

func (b *multiSeriesIter) Next() bool {
	if b.idx >= b.seriesCount {
		return false
	}

	b.idx++
	return true
}

func (b *multiSeriesIter) Remaining() int {
	return b.seriesCount - b.idx
}

func (b *multiSeriesIter) Current() SeriesIterator {
	return NewSeriesIterator(b.count, b.initialTime, b.stepSize, b.blockSize)
}
