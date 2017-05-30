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

package xserver

import (
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// Options provide a set of server options
type Options interface {
	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetRetryOptions sets the retry options
	SetRetryOptions(value xretry.Options) Options

	// RetryOptions returns the retry options
	RetryOptions() xretry.Options
}

type options struct {
	instrumentOpts instrument.Options
	retryOpts      xretry.Options
}

// NewOptions creates a new set of server options
func NewOptions() Options {
	return &options{
		instrumentOpts: instrument.NewOptions(),
		retryOpts:      xretry.NewOptions(),
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetRetryOptions(value xretry.Options) Options {
	opts := *o
	opts.retryOpts = value
	return &opts
}

func (o *options) RetryOptions() xretry.Options {
	return o.retryOpts
}
