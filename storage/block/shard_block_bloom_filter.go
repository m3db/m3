package block

// CloseFn represents a functioning for releasing resources associated with the
// bloom filter
type CloseFn func() error

type shardBlockBloomFilter struct {
	bloomFilter BloomFilter
	close       func() error
}

func (bf *shardBlockBloomFilter) Test(value []byte) bool {
	return bf.bloomFilter.Test(value)
}

func (bf *shardBlockBloomFilter) M() uint {
	return bf.bloomFilter.M()
}

func (bf *shardBlockBloomFilter) K() uint {
	return bf.bloomFilter.K()
}

func (bf *shardBlockBloomFilter) Close() error {
	return bf.close()
}

// NewShardBlockBloomFilter instantiates a new ShardBlockBloomFilter
func NewShardBlockBloomFilter(bloomFilter BloomFilter, closeFn CloseFn) ShardBlockBloomFilter {
	return &shardBlockBloomFilter{
		bloomFilter: bloomFilter,
		close:       closeFn,
	}
}
