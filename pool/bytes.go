package pool

import (
	"sort"

	"code.uber.internal/infra/memtsdb"
)

// TODO(r): instrument this to tune pooling
type bytesPool struct {
	sizesAsc          []memtsdb.PoolBucket
	buckets           []bytesPoolBucket
	maxBucketCapacity int
}

type bytesPoolBucket struct {
	capacity int
	values   chan []byte
}

// NewBytesPool creates a new pool
func NewBytesPool(sizes []memtsdb.PoolBucket) memtsdb.BytesPool {
	sizesAsc := make([]memtsdb.PoolBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(memtsdb.PoolBucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}
	return &bytesPool{sizesAsc: sizesAsc, maxBucketCapacity: maxBucketCapacity}
}

func (p *bytesPool) alloc(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func (p *bytesPool) Init() {
	buckets := make([]bytesPoolBucket, len(p.sizesAsc))
	for i := range p.sizesAsc {
		buckets[i].capacity = p.sizesAsc[i].Capacity
		buckets[i].values = make(chan []byte, p.sizesAsc[i].Count)
		for j := 0; j < p.sizesAsc[i].Count; j++ {
			buckets[i].values <- p.alloc(p.sizesAsc[i].Capacity)
		}
	}
	p.buckets = buckets
}

func (p *bytesPool) Get(capacity int) []byte {
	if capacity > p.maxBucketCapacity {
		return p.alloc(capacity)
	}
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			select {
			case b := <-p.buckets[i].values:
				return b
			default:
				// NB(r): use the bucket's capacity so can potentially
				// be returned to pool when it's finished with.
				return p.alloc(p.buckets[i].capacity)
			}
		}
	}
	return p.alloc(capacity)
}

func (p *bytesPool) Put(buffer []byte) {
	capacity := cap(buffer)
	if capacity > p.maxBucketCapacity {
		return
	}

	buffer = buffer[:0]
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			select {
			case p.buckets[i].values <- buffer:
				return
			default:
				return
			}
		}
	}
}
