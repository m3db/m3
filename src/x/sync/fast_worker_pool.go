package sync

import (
	"time"

	"github.com/m3db/m3/src/x/context"
)

type fastWorkerPool struct {
	workerPool WorkerPool
	batchSize  int
	count      int
}

var _ WorkerPool = &fastWorkerPool{}

func (p *fastWorkerPool) Init() {
	p.workerPool.Init()
}

func (p *fastWorkerPool) Go(work Work) {
	p.workerPool.Go(work)
}

func (p *fastWorkerPool) GoInstrument(work Work) ScheduleResult {
	return p.workerPool.GoInstrument(work)
}

func (p *fastWorkerPool) GoIfAvailable(work Work) bool {
	return p.workerPool.GoIfAvailable(work)
}

func (p *fastWorkerPool) GoWithTimeout(work Work, timeout time.Duration) bool {
	return p.workerPool.GoWithTimeout(work, timeout)
}

func (p *fastWorkerPool) GoWithTimeoutInstrument(work Work, timeout time.Duration) ScheduleResult {
	return p.workerPool.GoWithTimeoutInstrument(work, timeout)
}

func (p *fastWorkerPool) GoWithContext(ctx context.Context, work Work) ScheduleResult {
	p.count++
	if p.count == 1 {
		return p.workerPool.GoWithContext(ctx, work)

	}
	if p.count == p.batchSize {
		p.count = 0
	}
	return p.workerPool.GoInstrument(work)
}

func (p *fastWorkerPool) Fast(batchSize int) WorkerPool {
	return &fastWorkerPool{workerPool: p.workerPool, batchSize: batchSize}
}
