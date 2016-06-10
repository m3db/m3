package pool

import (
	"code.uber.internal/infra/memtsdb"
)

// TODO(r): instrument this to tune pooling
type objectPool struct {
	values chan interface{}
	alloc  memtsdb.PoolAllocator
}

// NewObjectPool creates a new pool
func NewObjectPool(size int) memtsdb.ObjectPool {
	return &objectPool{values: make(chan interface{}, size)}
}

func (p *objectPool) Init(alloc memtsdb.PoolAllocator) {
	capacity := cap(p.values)
	for i := 0; i < capacity; i++ {
		p.values <- alloc()
	}
	p.alloc = alloc
}

func (p *objectPool) Get() interface{} {
	select {
	case v := <-p.values:
		return v
	default:
		return p.alloc()
	}
}

func (p *objectPool) Put(obj interface{}) {
	select {
	case p.values <- obj:
		return
	default:
		return
	}
}
