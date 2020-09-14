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

package wide

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

const (
	defaultbatchSize = 1024
)

var (
	errDecodingOptionsUnset  = errors.New("decoding options unset")
	invalidBatchSizeTemplate = "batch size %d must be greater than 0"
)

type options struct {
	strict          bool
	batchSize       int
	bytesPool       pool.BytesPool
	decodingOptions msgpack.DecodingOptions
	instrumentOpts  instrument.Options
}

// NewOptions creates a new set of object pool options
func NewOptions() Options {
	return &options{
		batchSize:      defaultbatchSize,
		instrumentOpts: instrument.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.decodingOptions == nil {
		return errDecodingOptionsUnset
	}
	if o.batchSize < 1 {
		return fmt.Errorf(invalidBatchSizeTemplate, o.batchSize)
	}
	return nil
}

func (o *options) SetBatchSize(value int) Options {
	opts := *o
	opts.batchSize = value
	return &opts
}

func (o *options) Strict() bool {
	return o.strict
}

func (o *options) SetStrict(value bool) Options {
	opts := *o
	opts.strict = value
	return &opts
}

func (o *options) BatchSize() int {
	return o.batchSize
}

func (o *options) SetBytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.BytesPool {
	return o.bytesPool
}

func (o *options) SetDecodingOptions(value msgpack.DecodingOptions) Options {
	opts := *o
	opts.decodingOptions = value
	return &opts
}

func (o *options) DecodingOptions() msgpack.DecodingOptions {
	return o.decodingOptions
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}
