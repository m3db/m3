package arrow

type multiSeriesIterator interface {
	next() bool
	current() seriesIterator
	remaining() int
}

type multiSeriesIter struct {
	initialTime int64
	blockSize   int
	seriesCount int
	stepSize    int
	idx         int
	count       int
}

func newMultiSeriesIterator(
	seriesCount int, count int, initialTime int64, stepSize int, blockSize int,
) multiSeriesIterator {
	return &multiSeriesIter{
		idx:         -1,
		count:       count,
		stepSize:    stepSize,
		blockSize:   blockSize,
		seriesCount: seriesCount - 1,
		initialTime: initialTime,
	}
}

func (b *multiSeriesIter) next() bool {
	if b.idx >= b.seriesCount {
		return false
	}

	b.idx++
	return true
}

func (b *multiSeriesIter) remaining() int {
	return b.seriesCount - b.idx
}

func (b *multiSeriesIter) current() seriesIterator {
	return newSeriesIterator(b.count, b.initialTime, b.stepSize, b.blockSize)
}
