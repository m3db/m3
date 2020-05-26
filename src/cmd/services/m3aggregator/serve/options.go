// Copyright (c) 2020 Uber Technologies, Inc.
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

package serve

import (
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	m3msgserver "github.com/m3db/m3/src/aggregator/server/m3msg"
	rawtcpserver "github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
)

// Options are aggregator options.
type Options interface {
	// SetM3msgAddr sets the M3 message address.
	SetM3msgAddr(value string) Options

	// M3msgAddr returns the M3 message address.
	M3msgAddr() string

	// SetM3msgServerOpts sets the M3msgServerOpts.
	SetM3msgServerOpts(value m3msgserver.Options) Options

	// M3msgServerOpts returns the M3msgServerOpts.
	M3msgServerOpts() m3msgserver.Options

	// SetRawTCPAddr sets the RawTCP address.
	SetRawTCPAddr(value string) Options

	// RawTCPAddr returns the RawTCP address.
	RawTCPAddr() string

	// SetRawTCPServerOpts sets the RawTCPServerOpts.
	SetRawTCPServerOpts(value rawtcpserver.Options) Options

	// RawTCPServerOpts returns the RawTCPServerOpts.
	RawTCPServerOpts() rawtcpserver.Options

	// SetHTTPAddr sets the HTTP address.
	SetHTTPAddr(value string) Options

	// HTTPAddr returns the HTTP address.
	HTTPAddr() string

	// SetHTTPServerOpts sets the HTTPServerOpts.
	SetHTTPServerOpts(value httpserver.Options) Options

	// HTTPServerOpts returns the HTTPServerOpts.
	HTTPServerOpts() httpserver.Options

	// SetInstrumentOpts sets the InstrumentOpts.
	SetInstrumentOpts(value instrument.Options) Options

	// InstrumentOpts returns the InstrumentOpts.
	InstrumentOpts() instrument.Options

	// SetRWOptions sets RW options.
	SetRWOptions(value xio.Options) Options

	// RWOptions returns the RW options.
	RWOptions() xio.Options
}

type options struct {
	m3msgAddr        string
	m3msgServerOpts  m3msgserver.Options
	rawTCPAddr       string
	rawTCPServerOpts rawtcpserver.Options
	httpAddr         string
	httpServerOpts   httpserver.Options
	iOpts            instrument.Options
	rwOpts           xio.Options
}

// NewOptions creates a new aggregator server options.
func NewOptions(iOpts instrument.Options) Options {
	return &options{
		iOpts:  iOpts,
		rwOpts: xio.NewOptions(),
	}
}

func (o *options) SetM3msgAddr(value string) Options {
	opts := *o
	opts.m3msgAddr = value
	return &opts
}

func (o *options) M3msgAddr() string {
	return o.m3msgAddr
}

func (o *options) SetM3msgServerOpts(value m3msgserver.Options) Options {
	opts := *o
	opts.m3msgServerOpts = value
	return &opts
}

func (o *options) M3msgServerOpts() m3msgserver.Options {
	return o.m3msgServerOpts
}

func (o *options) SetRawTCPAddr(value string) Options {
	opts := *o
	opts.rawTCPAddr = value
	return &opts
}

func (o *options) RawTCPAddr() string {
	return o.rawTCPAddr
}

func (o *options) SetRawTCPServerOpts(value rawtcpserver.Options) Options {
	opts := *o
	opts.rawTCPServerOpts = value
	return &opts
}

func (o *options) RawTCPServerOpts() rawtcpserver.Options {
	return o.rawTCPServerOpts
}

func (o *options) SetHTTPAddr(value string) Options {
	opts := *o
	opts.httpAddr = value
	return &opts
}

func (o *options) HTTPAddr() string {
	return o.httpAddr
}

func (o *options) SetHTTPServerOpts(value httpserver.Options) Options {
	opts := *o
	opts.httpServerOpts = value
	return &opts
}

func (o *options) HTTPServerOpts() httpserver.Options {
	return o.httpServerOpts
}

func (o *options) SetInstrumentOpts(value instrument.Options) Options {
	opts := *o
	opts.iOpts = value
	return &opts
}

func (o *options) InstrumentOpts() instrument.Options {
	return o.iOpts
}

func (o *options) SetRWOptions(value xio.Options) Options {
	opts := *o
	opts.rwOpts = value
	return &opts
}

func (o *options) RWOptions() xio.Options {
	return o.rwOpts
}
