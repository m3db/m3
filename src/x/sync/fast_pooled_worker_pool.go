package sync

import (
	"context"
	"time"
)

type fastPooledWorkerPool struct {
	workerPool PooledWorkerPool
	batchSize  int
	count      int
}

var _ PooledWorkerPool = &fastPooledWorkerPool{}

func (f *fastPooledWorkerPool) Init() {
	f.workerPool.Init()
}

func (f *fastPooledWorkerPool) Go(work Work) {
	f.workerPool.Go(work)
}

func (f *fastPooledWorkerPool) GoWithTimeout(work Work, timeout time.Duration) bool {
	return f.GoWithTimeout(work, timeout)
}

func (f *fastPooledWorkerPool) GoWithContext(ctx context.Context, work Work) bool {
	f.count++
	if f.count == 1 {
		return f.workerPool.GoWithContext(ctx, work)
	}
	if f.count == f.batchSize {
		f.count = 0
	}
	f.workerPool.Go(work)
	return true
}

func (f *fastPooledWorkerPool) Fast(batchSize int) PooledWorkerPool {
	return &fastPooledWorkerPool{workerPool: f.workerPool, batchSize: batchSize}
}
