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

package m3db

import (
	"fmt"

	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/instrument"
)

type opts struct {
	iopts    instrument.Options
	nodeOpts node.Options
}

// NewOptions returns a new Options construct.
func NewOptions(
	iopts instrument.Options,
) Options {
	if iopts == nil {
		iopts = instrument.NewOptions()
	}
	return &opts{
		iopts: iopts,
	}
}

func (o *opts) Validate() error {
	if o.nodeOpts == nil {
		return fmt.Errorf("node options are not set")
	}

	return o.nodeOpts.Validate()
}

func (o *opts) SetInstrumentOptions(io instrument.Options) Options {
	o.iopts = io
	return o
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o *opts) SetNodeOptions(nopts node.Options) Options {
	o.nodeOpts = nopts
	return o
}

func (o *opts) NodeOptions() node.Options {
	return o.nodeOpts
}
