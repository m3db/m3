// Copyright (c) 2016 Uber Technologies, Inc.
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

package rawtcp

import (
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/server"
)

const (
	// A default limit value of 0 means error log rate limiting is disabled.
	defaultErrorLogLimitPerSecond = 0

	// The default read buffer size for raw TCP connections.
	defaultReadBufferSize = 65536
)

// Options provide a set of server options.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetServerOptions sets the server options.
	SetServerOptions(value server.Options) Options

	// ServerOptiosn returns the server options.
	ServerOptions() server.Options

	// SetProtobufUnaggregatedIteratorOptions is deprecated.
	SetProtobufUnaggregatedIteratorOptions(value protobuf.UnaggregatedOptions) Options

	// ProtobufUnaggregatedIteratorOptions is deprecated.
	ProtobufUnaggregatedIteratorOptions() protobuf.UnaggregatedOptions

	// SetReadBufferSize sets the read buffer size.
	SetReadBufferSize(value int) Options

	// ReadBufferSize returns the read buffer size.
	ReadBufferSize() int

	// SetErrorLogLimitPerSecond sets the error log limit per second.
	SetErrorLogLimitPerSecond(value int64) Options

	// ErrorLogLimitPerSecond returns the error log limit per second.
	ErrorLogLimitPerSecond() int64

	// SetRWOptions sets RW options.
	SetRWOptions(value xio.Options) Options

	// RWOptions returns the RW options.
	RWOptions() xio.Options
}

type options struct {
	clockOpts            clock.Options
	instrumentOpts       instrument.Options
	serverOpts           server.Options
	protobufItOpts       protobuf.UnaggregatedOptions
	readBufferSize       int
	errLogLimitPerSecond int64
	rwOpts               xio.Options
}

// NewOptions creates a new set of server options.
func NewOptions() Options {
	return &options{
		clockOpts:            clock.NewOptions(),
		instrumentOpts:       instrument.NewOptions(),
		serverOpts:           server.NewOptions(),
		protobufItOpts:       protobuf.NewUnaggregatedOptions(),
		readBufferSize:       defaultReadBufferSize,
		errLogLimitPerSecond: defaultErrorLogLimitPerSecond,
		rwOpts:               xio.NewOptions(),
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetServerOptions(value server.Options) Options {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *options) ServerOptions() server.Options {
	return o.serverOpts
}

func (o *options) SetProtobufUnaggregatedIteratorOptions(value protobuf.UnaggregatedOptions) Options {
	opts := *o
	opts.protobufItOpts = value
	return &opts
}

func (o *options) ProtobufUnaggregatedIteratorOptions() protobuf.UnaggregatedOptions {
	return o.protobufItOpts
}

func (o *options) SetReadBufferSize(value int) Options {
	opts := *o
	opts.readBufferSize = value
	return &opts
}

func (o *options) ReadBufferSize() int {
	return o.readBufferSize
}

func (o *options) SetErrorLogLimitPerSecond(value int64) Options {
	opts := *o
	opts.errLogLimitPerSecond = value
	return &opts
}

func (o *options) ErrorLogLimitPerSecond() int64 {
	return o.errLogLimitPerSecond
}

func (o *options) SetRWOptions(value xio.Options) Options {
	opts := *o
	opts.rwOpts = value
	return &opts
}

func (o *options) RWOptions() xio.Options {
	return o.rwOpts
}
