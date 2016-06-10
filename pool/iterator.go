package pool

import "code.uber.internal/infra/memtsdb"

// TODO(r): instrument this to tune pooling
type iteratorPool struct {
	pool memtsdb.ObjectPool
}

// NewIteratorPool creates a new pool
func NewIteratorPool(size int) memtsdb.IteratorPool {
	return &iteratorPool{pool: NewObjectPool(size)}
}

func (p *iteratorPool) Init(alloc memtsdb.IteratorAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *iteratorPool) Get() memtsdb.Iterator {
	return p.pool.Get().(memtsdb.Iterator)
}

func (p *iteratorPool) Put(iter memtsdb.Iterator) {
	p.pool.Put(iter)
}
