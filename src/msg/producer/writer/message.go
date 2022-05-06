// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"unsafe"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/protocol/proto"

	"go.uber.org/atomic"
)

type message struct {
	// rm is *producer.RefCountedMessage wrapped as an atomic pointer.
	// isAcked, expectedProcessAtNanos and rm have to be atomics, as operations on those are across different goroutines.
	rm                     atomic.UnsafePointer
	pb                     msgpb.Message
	meta                   metadata
	initNanos              int64
	retryAtNanos           int64
	expectedProcessAtNanos atomic.Int64
	retried                int
	isAcked                atomic.Bool
}

func newMessage() *message {
	return &message{}
}

// Set sets the message.
func (m *message) Set(meta metadata, rm *producer.RefCountedMessage, initNanos int64) {
	m.isAcked.Store(false)
	m.rm.Store(unsafe.Pointer(rm))
	m.initNanos = initNanos
	m.meta = meta
	m.ToProto(&m.pb)
}

// Close resets the states of the message.
func (m *message) Close() {
	m.rm.Store(nil)
	m.retryAtNanos = 0
	m.retried = 0
	m.ResetProto(&m.pb)
}

// InitNanos returns the nanosecond when the message was initiated.
func (m *message) InitNanos() int64 {
	return m.initNanos
}

// ExpectedProcessAtNanos returns the nanosecond when the message should be processed. Used to calculate processing lag
// in the system.
func (m *message) ExpectedProcessAtNanos() int64 {
	return m.expectedProcessAtNanos.Load()
}

// RetryAtNanos returns the timestamp for next retry in nano seconds.
func (m *message) RetryAtNanos() int64 {
	return m.retryAtNanos
}

// SetRetryAtNanos sets the next retry nanos.
func (m *message) SetRetryAtNanos(value int64) {
	var val int64
	if m.retryAtNanos > 0 {
		val = m.retryAtNanos
	} else {
		val = m.initNanos
	}
	m.expectedProcessAtNanos.Store(val)
	m.retryAtNanos = value
}

// WriteTimes returns the times the message has been written.
func (m *message) WriteTimes() int {
	return m.retried
}

// IncWriteTimes increments the times the message has been written and returns the value.
func (m *message) IncWriteTimes() int {
	m.retried++
	return m.retried
}

// IsAcked returns true if the message has been acked.
func (m *message) IsAcked() bool {
	return m.isAcked.Load()
}

// Ack acknowledges the message. Duplicated acks on the same message might cause panic.
func (m *message) Ack() {
	m.isAcked.Store(true)
	(*producer.RefCountedMessage)(m.rm.Load()).DecRef()
}

// RefCounted returns the unwrapped RefCountedMessage
func (m *message) RefCounted() *producer.RefCountedMessage {
	return (*producer.RefCountedMessage)(m.rm.Load())
}

func (m *message) ShardID() uint64 {
	return m.meta.shard
}

// Metadata returns the metadata.
func (m *message) Metadata() metadata {
	return m.meta
}

// SetSentAt sets the sentAtNanos on the metadata proto.
func (m *message) SetSentAt(nanos int64) {
	m.pb.Metadata.SentAtNanos = uint64(nanos)
}

// Marshaler returns the marshaler and a bool to indicate whether the marshaler is valid.
func (m *message) Marshaler() (proto.Marshaler, bool) {
	return &m.pb, !m.RefCounted().IsDroppedOrConsumed()
}

func (m *message) ToProto(pb *msgpb.Message) {
	m.meta.ToProto(&pb.Metadata)
	pb.Value = (*producer.RefCountedMessage)(m.rm.Load()).Bytes()
}

func (m *message) ResetProto(pb *msgpb.Message) {
	pb.Value = nil
}
