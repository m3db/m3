package base

// Datapoint is a basic datapoint.
type Datapoint struct {
	Value     float64
	Timestamp int64
}

// BlockIterator is a basic iterator across a single series block.
type BlockIterator interface {
	// Next moves to the next element in the iterator.
	Next() bool
	// Current yields the current value.
	Current() Datapoint
	// Remaining reports how many elements are remaining.
	Remaining() int
	// Close closes the iterator.
	Close()
}

type blockIter struct {
	initialTime int64
	stepSize    int
	blockSize   int
	idx         int
}

// NewBlockIterator creates a test block iterator.
func NewBlockIterator(initialTime int64, stepSize int, blockSize int) BlockIterator {
	return &blockIter{
		idx:         -1,
		blockSize:   blockSize,
		stepSize:    stepSize,
		initialTime: initialTime,
	}
}

func (b *blockIter) Next() bool {
	if b.idx*b.stepSize >= b.blockSize {
		return false
	}

	b.idx++
	return true
}

func (b *blockIter) Remaining() int {
	return b.blockSize/b.stepSize - b.idx - 1
}

func (b *blockIter) Current() Datapoint {
	return Datapoint{
		Value:     float64(b.idx),
		Timestamp: int64(b.idx*b.stepSize) + b.initialTime,
	}
}

func (b *blockIter) Close() {}
