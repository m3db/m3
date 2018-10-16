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

package server

import (
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3x/instrument"
	xserver "github.com/m3db/m3x/server"
)

const (
	defaultReadBufferSize = 1440
)

// Options provide a set of options for the server.
type Options interface {
	// SetHandlerOptions sets the handler options.
	SetHandlerOptions(value HandlerOptions) Options

	// HandlerOptions returns the handler options.
	HandlerOptions() HandlerOptions

	// SetTCPServerOptions sets the TCP server options.
	SetTCPServerOptions(value xserver.Options) Options

	// TCPServerOptions returns the TCP server options.
	TCPServerOptions() xserver.Options
}

type serverOptions struct {
	handlerOpts HandlerOptions
	serverOpts  xserver.Options
}

// NewOptions create a new set of server options.
func NewOptions() Options {
	return &serverOptions{
		handlerOpts: NewHandlerOptions(),
		serverOpts:  xserver.NewOptions(),
	}
}

func (o *serverOptions) SetHandlerOptions(value HandlerOptions) Options {
	opts := *o
	opts.handlerOpts = value
	return &opts
}

func (o *serverOptions) HandlerOptions() HandlerOptions {
	return o.handlerOpts
}

func (o *serverOptions) SetTCPServerOptions(value xserver.Options) Options {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *serverOptions) TCPServerOptions() xserver.Options {
	return o.serverOpts
}

// HandleFn handles a metric alongside its staged metadatas.
type HandleFn func(unaggregated.MetricUnion, metadata.StagedMetadatas) error

// HandlerOptions provide a set of options for the handler.
type HandlerOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) HandlerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetProtobufUnaggregatedIteratorOptions sets the protobuf unaggregated iterator options.
	SetProtobufUnaggregatedIteratorOptions(value protobuf.UnaggregatedOptions) HandlerOptions

	// ProtobufUnaggregatedIteratorOptions returns the protobuf unaggregated iterator options.
	ProtobufUnaggregatedIteratorOptions() protobuf.UnaggregatedOptions

	// SetReadBufferSize sets the read buffer size.
	SetReadBufferSize(value int) HandlerOptions

	// ReadBufferSize returns the read buffer size.
	ReadBufferSize() int

	// SetHandleFn sets the handle fn.
	SetHandleFn(value HandleFn) HandlerOptions

	// HandleFn returns the handle fn.
	HandleFn() HandleFn
}

type handlerOptions struct {
	handleFn       HandleFn
	instrumentOpts instrument.Options
	protobufItOpts protobuf.UnaggregatedOptions
	readBufferSize int
}

// NewHandlerOptions create a new set of options for the handler.
func NewHandlerOptions() HandlerOptions {
	return &handlerOptions{
		instrumentOpts: instrument.NewOptions(),
		protobufItOpts: protobuf.NewUnaggregatedOptions(),
		readBufferSize: defaultReadBufferSize,
	}
}

func (o *handlerOptions) SetInstrumentOptions(value instrument.Options) HandlerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *handlerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *handlerOptions) SetProtobufUnaggregatedIteratorOptions(
	value protobuf.UnaggregatedOptions,
) HandlerOptions {
	opts := *o
	opts.protobufItOpts = value
	return &opts
}

func (o *handlerOptions) ProtobufUnaggregatedIteratorOptions() protobuf.UnaggregatedOptions {
	return o.protobufItOpts
}

func (o *handlerOptions) SetReadBufferSize(value int) HandlerOptions {
	opts := *o
	opts.readBufferSize = value
	return &opts
}

func (o *handlerOptions) ReadBufferSize() int {
	return o.readBufferSize
}

func (o *handlerOptions) SetHandleFn(value HandleFn) HandlerOptions {
	opts := *o
	opts.handleFn = value
	return &opts
}

func (o *handlerOptions) HandleFn() HandleFn {
	return o.handleFn
}
