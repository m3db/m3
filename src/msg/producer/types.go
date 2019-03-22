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

package producer

import (
	"github.com/m3db/m3/src/cluster/services"
)

// FinalizeReason defines the reason why the message is being finalized by Producer.
type FinalizeReason int

const (
	// Consumed means the message has been fully consumed.
	Consumed FinalizeReason = iota

	// Dropped means the message has been dropped.
	Dropped
)

// Message contains the data that will be produced by the producer.
// It should only be finalized by the producer.
type Message interface {
	// Shard returns the shard of the message.
	Shard() uint32

	// Bytes returns the bytes of the message.
	Bytes() []byte

	// Size returns the size of the bytes of the message.
	Size() int

	// Finalize will be called by producer to indicate the end of its lifecycle.
	Finalize(FinalizeReason)
}

// CloseType decides how the producer should be closed.
type CloseType int

const (
	// WaitForConsumption blocks the close call until all the messages have been consumed.
	WaitForConsumption CloseType = iota
	// DropEverything will close the producer and drop all the messages that have not been consumed.
	DropEverything
)

// Producer produces message to a topic.
type Producer interface {
	// Produce produces the message.
	Produce(m Message) error

	// RegisterFilter registers a filter to a consumer service.
	RegisterFilter(sid services.ServiceID, fn FilterFunc)

	// UnregisterFilter unregisters the filter of a consumer service.
	UnregisterFilter(sid services.ServiceID)

	// NumShards returns the total number of shards of the topic the producer is
	// producing to.
	NumShards() uint32

	// Init initializes a producer.
	Init() error

	// Close stops the producer from accepting new requests immediately.
	// If the CloseType is WaitForConsumption, then it will block until all the messages have been consumed.
	// If the CloseType is DropEverything, then it will simply drop all the messages buffered and return.
	Close(ct CloseType)
}

// FilterFunc can filter message.
type FilterFunc func(m Message) bool

// Options configs a producer.
type Options interface {
	// Buffer returns the buffer.
	Buffer() Buffer

	// SetBuffer sets the buffer.
	SetBuffer(value Buffer) Options

	// Writer returns the writer.
	Writer() Writer

	// SetWriter sets the writer.
	SetWriter(value Writer) Options
}

// Buffer buffers all the messages in the producer.
type Buffer interface {
	// Add adds message to the buffer and returns a reference counted message.
	Add(m Message) (*RefCountedMessage, error)

	// Init initializes the buffer.
	Init()

	// Close stops the buffer from accepting new requests immediately.
	// If the CloseType is WaitForConsumption, then it will block until all the messages have been consumed.
	// If the CloseType is DropEverything, then it will simply drop all the messages buffered and return.
	Close(ct CloseType)
}

// Writer writes all the messages out to the consumer services.
type Writer interface {
	// Write writes a reference counted message out.
	Write(rm *RefCountedMessage) error

	// RegisterFilter registers a filter to a consumer service.
	RegisterFilter(sid services.ServiceID, fn FilterFunc)

	// UnregisterFilter unregisters the filter of a consumer service.
	UnregisterFilter(sid services.ServiceID)

	// NumShards returns the total number of shards of the topic the writer is
	// writing to.
	NumShards() uint32

	// Init initializes a writer.
	Init() error

	// Close closes the writer.
	Close()
}
