// Copyright (c) 2017 Uber Technologies, Inc.
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

package common

import (
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

const (
	defaultQueueSize              = 65536
	defaultConnectTimeout         = 2 * time.Second
	defaultConnectionKeepAlive    = true
	defaultConnectionWriteTimeout = 250 * time.Millisecond
)

// ConnectionOptions provide a set of connection options.
type ConnectionOptions interface {
	// SetConnectTimeout sets the connect timeout.
	SetConnectTimeout(value time.Duration) ConnectionOptions

	// ConnectTimeout returns the connect timeout.
	ConnectTimeout() time.Duration

	// SetConnectionKeepAlive sets the connection keepalive.
	SetConnectionKeepAlive(value bool) ConnectionOptions

	// ConnectionKeepAlive returns the connection keepAlive.
	ConnectionKeepAlive() bool

	// SetConnectionWriteTimeout sets the connection write timeout.
	SetConnectionWriteTimeout(value time.Duration) ConnectionOptions

	// ConnectionWriteTimeout returns the connection write timeout.
	ConnectionWriteTimeout() time.Duration

	// SetReconnectRetryOptions sets the reconnect retry options.
	SetReconnectRetryOptions(value retry.Options) ConnectionOptions

	// ReconnectRetryOptions returns the reconnect retry options.
	ReconnectRetryOptions() retry.Options
}

type connectionOptions struct {
	connectTimeout         time.Duration
	connectionKeepAlive    bool
	connectionWriteTimeout time.Duration
	reconnectRetryOpts     retry.Options
}

// NewConnectionOptions create a new set of connection options.
func NewConnectionOptions() ConnectionOptions {
	return &connectionOptions{
		connectTimeout:         defaultConnectTimeout,
		connectionKeepAlive:    defaultConnectionKeepAlive,
		connectionWriteTimeout: defaultConnectionWriteTimeout,
		reconnectRetryOpts:     retry.NewOptions(),
	}
}

func (o *connectionOptions) SetConnectTimeout(value time.Duration) ConnectionOptions {
	opts := *o
	opts.connectTimeout = value
	return &opts
}

func (o *connectionOptions) ConnectTimeout() time.Duration {
	return o.connectTimeout
}

func (o *connectionOptions) SetConnectionKeepAlive(value bool) ConnectionOptions {
	opts := *o
	opts.connectionKeepAlive = value
	return &opts
}

func (o *connectionOptions) ConnectionKeepAlive() bool {
	return o.connectionKeepAlive
}

func (o *connectionOptions) SetConnectionWriteTimeout(value time.Duration) ConnectionOptions {
	opts := *o
	opts.connectionWriteTimeout = value
	return &opts
}

func (o *connectionOptions) ConnectionWriteTimeout() time.Duration {
	return o.connectionWriteTimeout
}

func (o *connectionOptions) SetReconnectRetryOptions(value retry.Options) ConnectionOptions {
	opts := *o
	opts.reconnectRetryOpts = value
	return &opts
}

func (o *connectionOptions) ReconnectRetryOptions() retry.Options {
	return o.reconnectRetryOpts
}

// QueueOptions provide a set of queue options.
type QueueOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) QueueOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) QueueOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetConnectionOptions sets the connection options.
	SetConnectionOptions(value ConnectionOptions) QueueOptions

	// ConnectionOptions returns the connection options.
	ConnectionOptions() ConnectionOptions

	// SetQueueSize sets the queue size.
	SetQueueSize(value int) QueueOptions

	// QueueSize returns the queue size.
	QueueSize() int
}

type queueOptions struct {
	clockOpts      clock.Options
	instrumentOpts instrument.Options
	connectionOpts ConnectionOptions
	queueSize      int
}

// NewQueueOptions create a new set of queue options.
func NewQueueOptions() QueueOptions {
	return &queueOptions{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		connectionOpts: NewConnectionOptions(),
		queueSize:      defaultQueueSize,
	}
}

func (o *queueOptions) SetClockOptions(value clock.Options) QueueOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *queueOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *queueOptions) SetInstrumentOptions(value instrument.Options) QueueOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *queueOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *queueOptions) SetConnectionOptions(value ConnectionOptions) QueueOptions {
	opts := *o
	opts.connectionOpts = value
	return &opts
}

func (o *queueOptions) ConnectionOptions() ConnectionOptions {
	return o.connectionOpts
}

func (o *queueOptions) SetQueueSize(value int) QueueOptions {
	opts := *o
	opts.queueSize = value
	return &opts
}

func (o *queueOptions) QueueSize() int {
	return o.queueSize
}
