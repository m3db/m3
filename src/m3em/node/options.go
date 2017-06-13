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

package node

import (
	"fmt"
	"time"

	"github.com/m3db/m3x/instrument"
	xretry "github.com/m3db/m3x/retry"
)

var (
	defaultNodeOptTimeout     = 90 * time.Second
	defaultTransferBufferSize = 1024 * 1024       /* 1 MB */
	defaultMaxPullSize        = int64(1024 * 100) /* 100 KB*/
	fourMegaBytes             = 4 * 1024 * 1024
)

type nodeOpts struct {
	iopts              instrument.Options
	operationTimeout   time.Duration
	transferBufferSize int
	maxPullSize        int64
	retrier            xretry.Retrier
	hOpts              HeartbeatOptions
	opClientFn         OperatorClientFn
}

// NewOptions returns a new Options construct.
func NewOptions(
	opts instrument.Options,
) Options {
	if opts == nil {
		opts = instrument.NewOptions()
	}
	return &nodeOpts{
		iopts:              opts,
		operationTimeout:   defaultNodeOptTimeout,
		transferBufferSize: defaultTransferBufferSize,
		maxPullSize:        defaultMaxPullSize,
		retrier:            xretry.NewRetrier(xretry.NewOptions()),
		hOpts:              NewHeartbeatOptions(),
	}
}

func (o *nodeOpts) Validate() error {
	// grpc max message size (by default, 4MB). see also: https://github.com/grpc/grpc-go/issues/746
	if o.transferBufferSize >= fourMegaBytes {
		return fmt.Errorf("TransferBufferSize must be < 4MB")
	}

	if o.maxPullSize < 0 {
		return fmt.Errorf("MaxPullSize must be >= 0")
	}

	if o.opClientFn == nil {
		return fmt.Errorf("OperatorClientFn is not set")
	}

	return o.hOpts.Validate()
}

func (o *nodeOpts) SetInstrumentOptions(io instrument.Options) Options {
	o.iopts = io
	return o
}

func (o *nodeOpts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o *nodeOpts) SetRetrier(retrier xretry.Retrier) Options {
	o.retrier = retrier
	return o
}

func (o *nodeOpts) Retrier() xretry.Retrier {
	return o.retrier
}

func (o *nodeOpts) SetOperationTimeout(d time.Duration) Options {
	o.operationTimeout = d
	return o
}

func (o *nodeOpts) OperationTimeout() time.Duration {
	return o.operationTimeout
}

func (o *nodeOpts) SetTransferBufferSize(sz int) Options {
	o.transferBufferSize = sz
	return o
}

func (o *nodeOpts) TransferBufferSize() int {
	return o.transferBufferSize
}

func (o *nodeOpts) SetMaxPullSize(sz int64) Options {
	o.maxPullSize = sz
	return o
}

func (o *nodeOpts) MaxPullSize() int64 {
	return o.maxPullSize
}

func (o *nodeOpts) SetHeartbeatOptions(ho HeartbeatOptions) Options {
	o.hOpts = ho
	return o
}

func (o *nodeOpts) HeartbeatOptions() HeartbeatOptions {
	return o.hOpts
}

func (o *nodeOpts) SetOperatorClientFn(fn OperatorClientFn) Options {
	o.opClientFn = fn
	return o
}

func (o *nodeOpts) OperatorClientFn() OperatorClientFn {
	return o.opClientFn
}
