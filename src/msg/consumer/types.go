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
	"time"

	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
)

// Message carries the data that needs to be processed.
type Message interface {
	// Bytes returns the bytes.
	Bytes() []byte

	// Ack acks the message.
	Ack()
}

// Consumer receives messages from a connection.
type Consumer interface {
	// Message waits for and returns the next message received.
	Message() (Message, error)

	// Init initializes the consumer.
	Init()

	// Close closes the consumer.
	Close()
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

// Options configs the consumer listener.
type Options interface {
	// EncoderOptions returns the options for Encoder.
	EncoderOptions() proto.Options

	// SetEncoderOptions sets the options for Encoder.
	SetEncoderOptions(value proto.Options) Options

	// DecoderOptions returns the options for Decoder.
	DecoderOptions() proto.Options

	// SetDecoderOptions sets the options for Decoder.
	SetDecoderOptions(value proto.Options) Options

	// MessagePoolOptions returns the options for message pool.
	MessagePoolOptions() MessagePoolOptions

	// SetMessagePoolOptions sets the options for message pool.
	SetMessagePoolOptions(value MessagePoolOptions) Options

	// AckFlushInterval returns the ack flush interval.
	AckFlushInterval() time.Duration

	// SetAckFlushInterval sets the ack flush interval.
	SetAckFlushInterval(value time.Duration) Options

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

	// ConnectionWriteTimeout returns the timeout for writing to the connection.
	ConnectionWriteTimeout() time.Duration

	// SetConnectionWriteTimeout sets the write timeout for the connection.
	SetConnectionWriteTimeout(value time.Duration) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// Compression returns the compression method used.
	Compression() xio.CompressionMethod

	// SetCompression sets the compression method used.
	SetCompression(value xio.CompressionMethod) Options
}

// MessageProcessor processes the message. When a MessageProcessor was set in the
// server, it will be called to process every message received.
type MessageProcessor interface {
	Process(m Message)
	Close()
}

// ConsumeFn processes the consumer. This is useful when user want to reuse
// resource across messages received on the same consumer or have finer level
// control on how to read messages from consumer.
type ConsumeFn func(c Consumer)
