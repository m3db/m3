package block

// CloseFn represents a function for releasing resources associated with the
// bloom filter
type CloseFn func() error

type managedBloomFilter struct {
	bloomFilter bloomFilter
	close       func() error
}

func (bf *managedBloomFilter) Test(value []byte) bool {
	return bf.bloomFilter.Test(value)
}

func (bf *managedBloomFilter) M() uint {
	return bf.bloomFilter.M()
}

func (bf *managedBloomFilter) K() uint {
	return bf.bloomFilter.K()
}

func (bf *managedBloomFilter) Close() error {
	return bf.close()
}

// NewManagedBloomFilter instantiates a new ManagedBloomFilter
func NewManagedBloomFilter(bloomFilter bloomFilter, closeFn CloseFn) ManagedBloomFilter {
	return &managedBloomFilter{
		bloomFilter: bloomFilter,
		close:       closeFn,
	}
}
