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
	"time"

	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
)

var (
	defaultAckBufferSize        = 1048576
	defaultAckFlushInterval     = 200 * time.Millisecond
	defaultConnectionBufferSize = 1048576
	defaultWriteTimeout         = 5 * time.Second
)

type options struct {
	encOptions       proto.Options
	decOptions       proto.Options
	messagePoolOpts  MessagePoolOptions
	ackFlushInterval time.Duration
	ackBufferSize    int
	writeBufferSize  int
	readBufferSize   int
	writeTimeout     time.Duration
	iOpts            instrument.Options
	rwOpts           xio.Options
	compression      xio.CompressionMethod
}

// NewOptions creates a new options.
func NewOptions() Options {
	return &options{
		encOptions:       proto.NewOptions(),
		decOptions:       proto.NewOptions(),
		messagePoolOpts:  MessagePoolOptions{PoolOptions: pool.NewObjectPoolOptions()},
		ackFlushInterval: defaultAckFlushInterval,
		ackBufferSize:    defaultAckBufferSize,
		writeBufferSize:  defaultConnectionBufferSize,
		readBufferSize:   defaultConnectionBufferSize,
		writeTimeout:     defaultWriteTimeout,
		iOpts:            instrument.NewOptions(),
		rwOpts:           xio.NewOptions(),
	}
}

func (opts *options) EncoderOptions() proto.Options {
	return opts.encOptions
}

func (opts *options) SetEncoderOptions(value proto.Options) Options {
	o := *opts
	o.encOptions = value
	return &o
}

func (opts *options) DecoderOptions() proto.Options {
	return opts.decOptions
}

func (opts *options) SetDecoderOptions(value proto.Options) Options {
	o := *opts
	o.decOptions = value
	return &o
}

func (opts *options) MessagePoolOptions() MessagePoolOptions {
	return opts.messagePoolOpts
}

func (opts *options) SetMessagePoolOptions(value MessagePoolOptions) Options {
	o := *opts
	o.messagePoolOpts = value
	return &o
}

func (opts *options) AckFlushInterval() time.Duration {
	return opts.ackFlushInterval
}

func (opts *options) SetAckFlushInterval(value time.Duration) Options {
	o := *opts
	o.ackFlushInterval = value
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

func (opts *options) ConnectionWriteTimeout() time.Duration {
	return opts.writeTimeout
}

func (opts *options) SetConnectionWriteTimeout(value time.Duration) Options {
	o := *opts
	o.writeTimeout = value
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

func (opts *options) SetRWOptions(value xio.Options) Options {
	o := *opts
	o.rwOpts = value
	return &o
}

func (opts *options) RWOptions() xio.Options {
	return opts.rwOpts
}

func (opts *options) SetCompression(value xio.CompressionMethod) Options {
	o := *opts
	o.compression = value
	return &o
}

func (opts *options) Compression() xio.CompressionMethod {
	return opts.compression
}
