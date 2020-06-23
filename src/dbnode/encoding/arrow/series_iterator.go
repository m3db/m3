package arrow

type seriesIterator interface {
	next() bool
	current() blockIterator
	remaining() int
}

type seriesIter struct {
	initialTime int64
	blockSize   int
	idx         int
	count       int
}

func newSeriesIter(count int, initialTime int64, blockSize int) seriesIterator {
	return &seriesIter{
		idx:         -1,
		count:       count,
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
	return b.blockSize - b.idx + 1
}

func (b *seriesIter) current() blockIterator {
	init := b.initialTime + int64((b.idx+1)*b.blockSize)
	return newBlockIterator(init, b.blockSize)
}
