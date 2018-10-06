package m3msg

import (
	"sync/atomic"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3msg/consumer"
)

// WriteFn is the function that writes a metric.
type WriteFn func(
	id []byte,
	metricTimeNanos int64,
	value float64,
	sp policy.StoragePolicy,
	callback *RefCountedCallback,
)

// CallbackType defines the type for the callback.
type CallbackType int

// Supported CallbackTypes.
const (
	OnError CallbackType = iota
	OnSuccess
)

// RefCountedCallback wraps a message with a reference count, the message will
// be acked once the reference count decrements to zero.
type RefCountedCallback struct {
	ref int32
	msg consumer.Message
}

// NewRefCountedCallback creates a RefCountedCallback.
func NewRefCountedCallback(msg consumer.Message) *RefCountedCallback {
	return &RefCountedCallback{
		ref: 0,
		msg: msg,
	}
}

// incRef increments the ref count.
func (r *RefCountedCallback) incRef() {
	atomic.AddInt32(&r.ref, 1)
}

// decRef decrements the ref count. If the reference count became zero after
// the call, the message will be acked.
func (r *RefCountedCallback) decRef() {
	ref := atomic.AddInt32(&r.ref, -1)
	if ref == 0 {
		r.msg.Ack()
		return
	}
	if ref < 0 {
		panic("invalid ref count")
	}
}

// Callback performs the callback.
func (r *RefCountedCallback) Callback(t CallbackType) {
	if t == OnSuccess {
		r.decRef()
	}
}
