package client

import "sync/atomic"

type refCounter struct {
	doneFn func()
	n      int32
}

func (r *refCounter) incRef() {
	if atomic.AddInt32(&r.n, 1) <= 0 {
		panic("invalid rc state")
	}
}

func (r *refCounter) decRef() {
	if v := atomic.AddInt32(&r.n, -1); v == 0 {
		r.doneFn()
	} else if v < 0 {
		panic("invalid rc state")
	}
}
