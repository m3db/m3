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
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/server"
)

var (
	defaultAckBufferSize        = 100
	defaultConnectionBufferSize = 16384
)

type options struct {
	encdecOptions   proto.EncodeDecoderOptions
	messagePoolOpts pool.ObjectPoolOptions
	ackBufferSize   int
	writeBufferSize int
	readBufferSize  int
	iOpts           instrument.Options
}

// NewOptions creates a new options.
func NewOptions() Options {
	return &options{
		encdecOptions:   proto.NewEncodeDecoderOptions(),
		messagePoolOpts: pool.NewObjectPoolOptions(),
		ackBufferSize:   defaultAckBufferSize,
		writeBufferSize: defaultConnectionBufferSize,
		readBufferSize:  defaultConnectionBufferSize,
		iOpts:           instrument.NewOptions(),
	}
}

func (opts *options) EncodeDecoderOptions() proto.EncodeDecoderOptions {
	return opts.encdecOptions
}

func (opts *options) SetEncodeDecoderOptions(value proto.EncodeDecoderOptions) Options {
	o := *opts
	o.encdecOptions = value
	return &o
}

func (opts *options) MessagePoolOptions() pool.ObjectPoolOptions {
	return opts.messagePoolOpts
}

func (opts *options) SetMessagePoolOptions(value pool.ObjectPoolOptions) Options {
	o := *opts
	o.messagePoolOpts = value
	return &o
}

func (opts *options) AckBufferSize() int {
	return opts.ackBufferSize
}

func (opts *options) SetAckBufferSize(value int) Options {
	o := *opts
	o.ackBufferSize = value
	return &o
}

func (opts *options) ConnectionWriteBufferSize() int {
	return opts.writeBufferSize
}

func (opts *options) SetConnectionWriteBufferSize(value int) Options {
	o := *opts
	o.writeBufferSize = value
	return &o
}

func (opts *options) ConnectionReadBufferSize() int {
	return opts.readBufferSize
}

func (opts *options) SetConnectionReadBufferSize(value int) Options {
	o := *opts
	o.readBufferSize = value
	return &o
}

func (opts *options) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *options) SetInstrumentOptions(value instrument.Options) Options {
	o := *opts
	o.iOpts = value
	return &o
}

type serverOptions struct {
	consumeFn ConsumeFn
	sOpts     server.Options
	cOpts     Options
}

// NewServerOptions creates ServerOptions.
func NewServerOptions() ServerOptions {
	return &serverOptions{
		sOpts: server.NewOptions(),
		cOpts: NewOptions(),
	}
}

func (opts *serverOptions) ConsumeFn() ConsumeFn {
	return opts.consumeFn
}

func (opts *serverOptions) SetConsumeFn(value ConsumeFn) ServerOptions {
	o := *opts
	o.consumeFn = value
	return &o
}

func (opts *serverOptions) ServerOptions() server.Options {
	return opts.sOpts
}

func (opts *serverOptions) SetServerOptions(value server.Options) ServerOptions {
	o := *opts
	o.sOpts = value
	return &o
}

func (opts *serverOptions) ConsumerOptions() Options {
	return opts.cOpts
}

func (opts *serverOptions) SetConsumerOptions(value Options) ServerOptions {
	o := *opts
	o.cOpts = value
	return &o
}
