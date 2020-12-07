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

package node

import (
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// NewTChanChannelFn creates a tchan channel.
type NewTChanChannelFn func(
	service Service,
	channelName string,
	opts *tchannel.ChannelOptions,
) (*tchannel.Channel, error)

func defaultTChanChannelFn(
	service Service,
	channelName string,
	opts *tchannel.ChannelOptions,
) (*tchannel.Channel, error) {
	return tchannel.NewChannel(channelName, opts)
}

// NewTChanNodeServerFn creates a tchan node server.
type NewTChanNodeServerFn func(
	service Service,
	iOpts instrument.Options,
) thrift.TChanServer

func defaultTChanNodeServerFn(
	service Service,
	_ instrument.Options,
) thrift.TChanServer {
	return rpc.NewTChanNodeServer(service)
}

// Options are thrift options.
type Options interface {
	// SetChannelOptions sets a tchan channel options.
	SetChannelOptions(value *tchannel.ChannelOptions) Options

	// ChannelOptions returns the tchan channel options.
	ChannelOptions() *tchannel.ChannelOptions

	// SetTChanChannelFn sets a tchan node channel registration.
	SetTChanChannelFn(value NewTChanChannelFn) Options

	// TChanChannelFn returns a tchan node channel registration.
	TChanChannelFn() NewTChanChannelFn

	// SetTChanNodeServerFn sets a tchan node server builder.
	SetTChanNodeServerFn(value NewTChanNodeServerFn) Options

	// TChanNodeServerFn returns a tchan node server builder.
	TChanNodeServerFn() NewTChanNodeServerFn

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options
}

type options struct {
	channelOptions    *tchannel.ChannelOptions
	instrumentOpts    instrument.Options
	tchanChannelFn    NewTChanChannelFn
	tchanNodeServerFn NewTChanNodeServerFn
}

// NewOptions creates a new options.
func NewOptions(chanOpts *tchannel.ChannelOptions) Options {
	return &options{
		channelOptions:    chanOpts,
		tchanChannelFn:    defaultTChanChannelFn,
		tchanNodeServerFn: defaultTChanNodeServerFn,
	}
}
func (o *options) SetChannelOptions(value *tchannel.ChannelOptions) Options {
	opts := *o
	opts.channelOptions = value
	return &opts
}

func (o *options) ChannelOptions() *tchannel.ChannelOptions {
	return o.channelOptions
}

func (o *options) SetTChanChannelFn(value NewTChanChannelFn) Options {
	opts := *o
	opts.tchanChannelFn = value
	return &opts
}

func (o *options) TChanChannelFn() NewTChanChannelFn {
	return o.tchanChannelFn
}

func (o *options) SetTChanNodeServerFn(value NewTChanNodeServerFn) Options {
	opts := *o
	opts.tchanNodeServerFn = value
	return &opts
}

func (o *options) TChanNodeServerFn() NewTChanNodeServerFn {
	return o.tchanNodeServerFn
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}
