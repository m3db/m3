package pool

import (
	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/context"
)

// TODO(r): instrument this to tune pooling
type contextPool struct {
	pool memtsdb.ObjectPool
}

// NewContextPool creates a new pool
func NewContextPool(size int) memtsdb.ContextPool {
	return &contextPool{pool: NewObjectPool(size)}
}

func (p *contextPool) Init() {
	p.pool.Init(func() interface{} {
		return context.NewPooledContext(p)
	})
}

func (p *contextPool) Get() memtsdb.Context {
	return p.pool.Get().(memtsdb.Context)
}

func (p *contextPool) Put(context memtsdb.Context) {
	p.pool.Put(context)
}
