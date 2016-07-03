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

package integration

import (
	"time"
)

const (
	// defaultServerStateChangeTimeout is the default time we wait for a server to change its state.
	defaultServerStateChangeTimeout = 30 * time.Second

	// defaultReadRequestTimeout is the default read request timeout.
	defaultReadRequestTimeout = 5 * time.Second

	// defaultWriteRequestTimeout is the default write request timeout.
	defaultWriteRequestTimeout = 5 * time.Second

	// defaultUseTChannelClientForReading determines whether we use the tchannel client for reading by default.
	defaultUseTChannelClientForReading = true

	// defaultUseTChannelClientForWriting determines whether we use the tchannel client for writing by default.
	defaultUseTChannelClientForWriting = false
)

type testOptions interface {
	// ServerStateChangeTimeout sets the server state change timeout.
	ServerStateChangeTimeout(value time.Duration) testOptions

	// GetServerStateChangeTimeout returns the server state change timeout.
	GetServerStateChangeTimeout() time.Duration

	// ReadRequestTimeout sets the read request timeout.
	ReadRequestTimeout(value time.Duration) testOptions

	// GetReadRequestTimeout returns the read request timeout.
	GetReadRequestTimeout() time.Duration

	// WriteRequestTimeout sets the write request timeout.
	WriteRequestTimeout(value time.Duration) testOptions

	// GetWriteRequestTimeout returns the write request timeout.
	GetWriteRequestTimeout() time.Duration

	// UseTChannelClientForReading sets whether we use the tchannel client for reading.
	UseTChannelClientForReading(value bool) testOptions

	// GetUseTChannelClientForReading returns whether we use the tchannel client for reading.
	GetUseTChannelClientForReading() bool

	// UseTChannelClientForWriting sets whether we use the tchannel client for writing.
	UseTChannelClientForWriting(value bool) testOptions

	// GetUseTChannelClientForWriting returns whether we use the tchannel client for writing.
	GetUseTChannelClientForWriting() bool
}

type options struct {
	serverStateChangeTimeout    time.Duration
	readRequestTimeout          time.Duration
	writeRequestTimeout         time.Duration
	useTChannelClientForReading bool
	useTChannelClientForWriting bool
}

func newOptions() testOptions {
	return &options{
		serverStateChangeTimeout:    defaultServerStateChangeTimeout,
		readRequestTimeout:          defaultReadRequestTimeout,
		writeRequestTimeout:         defaultWriteRequestTimeout,
		useTChannelClientForReading: defaultUseTChannelClientForReading,
		useTChannelClientForWriting: defaultUseTChannelClientForWriting,
	}
}

func (o *options) ServerStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) GetServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) ReadRequestTimeout(value time.Duration) testOptions {
	opts := *o
	opts.readRequestTimeout = value
	return &opts
}

func (o *options) GetReadRequestTimeout() time.Duration {
	return o.readRequestTimeout
}

func (o *options) WriteRequestTimeout(value time.Duration) testOptions {
	opts := *o
	opts.writeRequestTimeout = value
	return &opts
}

func (o *options) GetWriteRequestTimeout() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) UseTChannelClientForReading(value bool) testOptions {
	opts := *o
	opts.useTChannelClientForReading = value
	return &opts
}

func (o *options) GetUseTChannelClientForReading() bool {
	return o.useTChannelClientForReading
}

func (o *options) UseTChannelClientForWriting(value bool) testOptions {
	opts := *o
	opts.useTChannelClientForWriting = value
	return &opts
}

func (o *options) GetUseTChannelClientForWriting() bool {
	return o.useTChannelClientForWriting
}
