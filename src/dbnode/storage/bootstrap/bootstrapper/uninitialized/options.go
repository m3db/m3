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

// Package uninitialized implements uninitialized topology bootstrapping.
package uninitialized

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errNoResultOptions     = errors.New("result options not set")
	errNoInstrumentOptions = errors.New("instrument options not set")
)

type options struct {
	resultOpts result.Options
	iOpts      instrument.Options
}

// NewOptions creates a new Options.
func NewOptions() Options {
	return &options{
		resultOpts: result.NewOptions(),
		iOpts:      instrument.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.resultOpts == nil {
		return errNoResultOptions
	}
	if o.iOpts == nil {
		return errNoInstrumentOptions
	}
	return nil
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.iOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.iOpts
}
