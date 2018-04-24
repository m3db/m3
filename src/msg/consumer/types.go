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

package consumer

import (
	"net"

	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/pool"
)

// Message carries the data that needs to be processed.
type Message interface {
	// Bytes returns the bytes.
	Bytes() []byte

	// Ack acks the message.
	Ack()
}

// Listener is a consumer listener based on a network address.
type Listener interface {
	// Accept waits for and returns the next connection based consumer.
	Accept() (Consumer, error)

	// Close closes the listener.
	// Any blocked Accept operations will be unblocked and return errors.
	Close() error

	// Addr returns the listener's network address.
	Addr() net.Addr
}

// Consumer receives messages from a connection.
type Consumer interface {
	// Message waits for and returns the next message received.
	Message() (Message, error)

	// Close closes the consumer.
	Close()
}

// Options configs the consumer listener.
type Options interface {
	// EncodeDecoderOptions returns the options for EncodeDecoder.
	EncodeDecoderOptions() proto.EncodeDecoderOptions

	// SetEncodeDecoderOptions sets the options for EncodeDecoder.
	SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options

	// MessagePoolOptions returns the options for message pool.
	MessagePoolOptions() pool.ObjectPoolOptions

	// SetMessagePoolOptions sets the options for message pool.
	SetMessagePoolOptions(value pool.ObjectPoolOptions) Options

	// AckBufferSize returns the ack buffer size.
	AckBufferSize() int

	// SetAckBufferSize sets the ack buffer size.
	SetAckBufferSize(value int) Options

	// ConnectionWriteBufferSize returns the size of buffer before a write or a read.
	ConnectionWriteBufferSize() int

	// SetConnectionWriteBufferSize sets the buffer size.
	SetConnectionWriteBufferSize(value int) Options

	// ConnectionReadBufferSize returns the size of buffer before a write or a read.
	ConnectionReadBufferSize() int

	// SetConnectionWriteBufferSize sets the buffer size.
	SetConnectionReadBufferSize(value int) Options
}
