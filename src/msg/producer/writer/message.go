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
	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/protocol/proto"

	"go.uber.org/atomic"
)

type message struct {
	producer.RefCountedData

	pb           msgpb.Message
	meta         metadata
	retryAtNanos *atomic.Int64
	retried      *atomic.Int64
	isAcked      *atomic.Bool
}

func newMessage() *message {
	return &message{
		retryAtNanos: atomic.NewInt64(0),
		retried:      atomic.NewInt64(0),
		isAcked:      atomic.NewBool(false),
	}
}

// Reset resets the message.
func (m *message) Reset(meta metadata, data producer.RefCountedData) {
	m.meta = meta
	m.RefCountedData = data
	m.ToProto(&m.pb)
	m.retryAtNanos.Store(0)
	m.retried.Store(0)
	m.isAcked.Store(false)
}

// RetryAtNanos returns the timestamp for next retry in nano seconds.
func (m *message) RetryAtNanos() int64 {
	return m.retryAtNanos.Load()
}

// SetRetryAtNanos sets the next retry nanos.
func (m *message) SetRetryAtNanos(value int64) {
	m.retryAtNanos.Store(value)
}

// WriteTimes returns the times the message has been written.
func (m *message) WriteTimes() int64 {
	return m.retried.Load()
}

// IncWriteTimes increments the times the message has been written.
func (m *message) IncWriteTimes() {
	m.retried.Inc()
}

// IsDroppedOrAcked returns true if the message has been dropped or acked.
func (m *message) IsDroppedOrAcked() bool {
	return m.isAcked.Load() || m.RefCountedData.IsDroppedOrConsumed()
}

// Ack acknowledges the message. Duplicated acks on the same message might cause panic.
func (m *message) Ack() {
	m.RefCountedData.DecRef()
	m.isAcked.Store(true)
}

// Metadata returns the metadata.
func (m *message) Metadata() metadata {
	return m.meta
}

// Marshaler returns the marshaler and a bool to indicate whether the marshaler is valid.
func (m *message) Marshaler() (proto.Marshaler, bool) {
	return &m.pb, !m.RefCountedData.IsDroppedOrConsumed()
}

func (m *message) ToProto(pb *msgpb.Message) {
	m.meta.ToProto(&pb.Metadata)
	pb.Value = m.RefCountedData.Bytes()
}
