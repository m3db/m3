package bench

// RequestGenerator generates request descriptions on demand
type RequestGenerator func() *RequestDescription

// RequestDescription describes a request
type RequestDescription struct {
	StartUnixMs int64
	EndUnixMs   int64
	IDs         []string
}

// WorkerPool is a bounded pool of work
type WorkerPool struct {
	ch chan struct{}
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(size int) *WorkerPool {
	pool := &WorkerPool{
		ch: make(chan struct{}, size),
	}
	for i := 0; i < size; i++ {
		pool.ch <- struct{}{}
	}
	return pool
}

// Go will launch work and on reaching bounds will block until running
func (p *WorkerPool) Go(f func()) {
	s := <-p.ch
	go func() {
		f()
		p.ch <- s
	}()
}
