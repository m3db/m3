package arrow

type blockIterator interface {
	next() bool
	current() dp
	remaining() int
	close()
}

type blockIter struct {
	initialTime int64
	stepSize    int
	blockSize   int
	idx         int
}

func newBlockIterator(initialTime int64, stepSize int, blockSize int) blockIterator {
	return &blockIter{
		idx:         -1,
		blockSize:   blockSize,
		stepSize:    stepSize,
		initialTime: initialTime,
	}
}

func (b *blockIter) next() bool {
	if b.idx*b.stepSize >= b.blockSize {
		return false
	}

	b.idx++
	return true
}

func (b *blockIter) remaining() int {
	return b.blockSize/b.stepSize - b.idx - 1
}

func (b *blockIter) current() dp {
	return dp{
		val: float64(b.idx),
		ts:  int64(b.idx*b.stepSize) + b.initialTime,
	}
}

func (b *blockIter) close() {}
