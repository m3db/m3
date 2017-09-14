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

package handler

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

// ForwardHandlerOptions provide a set of options for the forwarding handler.
type ForwardHandlerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) ForwardHandlerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) ForwardHandlerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetQueueSize sets the queue size.
	SetQueueSize(value int) ForwardHandlerOptions

	// QueueSize returns the queue size.
	QueueSize() int

	// SetConnectTimeout sets the connect timeout.
	SetConnectTimeout(value time.Duration) ForwardHandlerOptions

	// ConnectTimeout returns the connect timeout.
	ConnectTimeout() time.Duration

	// SetConnectionKeepAlive sets the connection keepalive.
	SetConnectionKeepAlive(value bool) ForwardHandlerOptions

	// ConnectionKeepAlive returns the connection keepAlive.
	ConnectionKeepAlive() bool

	// SetConnectionWriteTimeout sets the connection write timeout.
	SetConnectionWriteTimeout(value time.Duration) ForwardHandlerOptions

	// ConnectionWriteTimeout returns the connection write timeout.
	ConnectionWriteTimeout() time.Duration

	// SetReconnectRetrier sets the reconnect retrier.
	SetReconnectRetrier(value retry.Retrier) ForwardHandlerOptions

	// ReconnectRetrier returns the reconnect retrier.
	ReconnectRetrier() retry.Retrier
}

type forwardHandlerOptions struct {
	clockOpts              clock.Options
	instrumentOpts         instrument.Options
	queueSize              int
	connectTimeout         time.Duration
	connectionKeepAlive    bool
	connectionWriteTimeout time.Duration
	reconnectRetrier       retry.Retrier
}

// NewForwardHandlerOptions create a new set of forward handler options.
func NewForwardHandlerOptions() ForwardHandlerOptions {
	return &forwardHandlerOptions{
		clockOpts:              clock.NewOptions(),
		instrumentOpts:         instrument.NewOptions(),
		queueSize:              defaultQueueSize,
		connectTimeout:         defaultConnectTimeout,
		connectionKeepAlive:    defaultConnectionKeepAlive,
		connectionWriteTimeout: defaultConnectionWriteTimeout,
		reconnectRetrier:       retry.NewRetrier(retry.NewOptions().SetForever(true)),
	}
}

func (o *forwardHandlerOptions) SetClockOptions(value clock.Options) ForwardHandlerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *forwardHandlerOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *forwardHandlerOptions) SetInstrumentOptions(value instrument.Options) ForwardHandlerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *forwardHandlerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *forwardHandlerOptions) SetQueueSize(value int) ForwardHandlerOptions {
	opts := *o
	opts.queueSize = value
	return &opts
}

func (o *forwardHandlerOptions) QueueSize() int {
	return o.queueSize
}

func (o *forwardHandlerOptions) SetConnectTimeout(value time.Duration) ForwardHandlerOptions {
	opts := *o
	opts.connectTimeout = value
	return &opts
}

func (o *forwardHandlerOptions) ConnectTimeout() time.Duration {
	return o.connectTimeout
}

func (o *forwardHandlerOptions) SetConnectionKeepAlive(value bool) ForwardHandlerOptions {
	opts := *o
	opts.connectionKeepAlive = value
	return &opts
}

func (o *forwardHandlerOptions) ConnectionKeepAlive() bool {
	return o.connectionKeepAlive
}

func (o *forwardHandlerOptions) SetConnectionWriteTimeout(value time.Duration) ForwardHandlerOptions {
	opts := *o
	opts.connectionWriteTimeout = value
	return &opts
}

func (o *forwardHandlerOptions) ConnectionWriteTimeout() time.Duration {
	return o.connectionWriteTimeout
}

func (o *forwardHandlerOptions) SetReconnectRetrier(value retry.Retrier) ForwardHandlerOptions {
	opts := *o
	opts.reconnectRetrier = value
	return &opts
}

func (o *forwardHandlerOptions) ReconnectRetrier() retry.Retrier {
	return o.reconnectRetrier
}
