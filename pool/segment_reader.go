package pool

import (
	"code.uber.internal/infra/memtsdb"
	xio "code.uber.internal/infra/memtsdb/x/io"
)

// TODO(r): instrument this to tune pooling
type segmentReaderPool struct {
	pool memtsdb.ObjectPool
}

// NewSegmentReaderPool creates a new pool
func NewSegmentReaderPool(size int) memtsdb.SegmentReaderPool {
	return &segmentReaderPool{pool: NewObjectPool(size)}
}

func (p *segmentReaderPool) Init() {
	p.pool.Init(func() interface{} {
		return xio.NewPooledSegmentReader(memtsdb.Segment{}, p)
	})
}

func (p *segmentReaderPool) Get() memtsdb.SegmentReader {
	return p.pool.Get().(memtsdb.SegmentReader)
}

func (p *segmentReaderPool) Put(reader memtsdb.SegmentReader) {
	p.pool.Put(reader)
}
