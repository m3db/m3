package arrow

type blockIterator interface {
	next() bool
	current() dp
	remaining() int
}

type blockIter struct {
	initialTime int64
	blockSize   int
	idx         int
}

func newBlockIterator(initialTime int64, blockSize int) blockIterator {
	return &blockIter{
		idx:         -1,
		blockSize:   blockSize,
		initialTime: initialTime,
	}
}

func (b *blockIter) next() bool {
	if b.idx >= b.blockSize {
		return false
	}

	b.idx++
	return true
}

func (b *blockIter) remaining() int {
	return b.blockSize - b.idx + 1
}

func (b *blockIter) current() dp {
	return dp{
		val: float64(b.idx),
		ts:  int64(b.idx) + b.initialTime,
	}
}
