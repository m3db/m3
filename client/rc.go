package client

import "sync/atomic"

type rc struct {
	d func()
	n int32
}

func (r *rc) incref() {
	if atomic.AddInt32(&r.n, 1) <= 0 {
		panic("invalid rc state")
	}
}

func (r *rc) decref() {
	if v := atomic.AddInt32(&r.n, -1); v == 0 {
		r.d()
	} else if v < 0 {
		panic("invalid rc state")
	}
}
