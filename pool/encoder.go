package pool

import "code.uber.internal/infra/memtsdb"

// TODO(r): instrument this to tune pooling
type encoderPool struct {
	pool memtsdb.ObjectPool
}

// NewEncoderPool creates a new pool
func NewEncoderPool(size int) memtsdb.EncoderPool {
	return &encoderPool{pool: NewObjectPool(size)}
}

func (p *encoderPool) Init(alloc memtsdb.EncoderAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *encoderPool) Get() memtsdb.Encoder {
	return p.pool.Get().(memtsdb.Encoder)
}

func (p *encoderPool) Put(encoder memtsdb.Encoder) {
	p.pool.Put(encoder)
}
