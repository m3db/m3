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

package proto

import (
	"github.com/m3db/m3x/pool"
)

const (
	// Using 16K which provides much better performance comparing
	// to lower values like 1k ~ 8k.
	defaultBufferSize = 16384
)

// NewBaseOptions creates a new BaseOptions.
func NewBaseOptions() BaseOptions {
	return &baseOptions{
		bufferSize: defaultBufferSize,
	}
}

type baseOptions struct {
	bytesPool  pool.BytesPool
	bufferSize int
}

func (opts *baseOptions) BytesPool() pool.BytesPool {
	return opts.bytesPool
}

func (opts *baseOptions) SetBytesPool(value pool.BytesPool) BaseOptions {
	o := *opts
	o.bytesPool = value
	return &o
}

func (opts *baseOptions) BufferSize() int {
	return opts.bufferSize
}

func (opts *baseOptions) SetBufferSize(value int) BaseOptions {
	o := *opts
	o.bufferSize = value
	return &o
}

// NewEncodeDecoderOptions creates an EncodeDecoderOptions.
func NewEncodeDecoderOptions() EncodeDecoderOptions {
	return &encdecOptions{
		encOpts: NewBaseOptions(),
		decOpts: NewBaseOptions(),
	}
}

type encdecOptions struct {
	encOpts BaseOptions
	decOpts BaseOptions
	pool    EncodeDecoderPool
}

func (opts *encdecOptions) EncoderOptions() BaseOptions {
	return opts.encOpts
}

func (opts *encdecOptions) SetEncoderOptions(value BaseOptions) EncodeDecoderOptions {
	o := *opts
	o.encOpts = value
	return &o
}

func (opts *encdecOptions) DecoderOptions() BaseOptions {
	return opts.decOpts
}

func (opts *encdecOptions) SetDecoderOptions(value BaseOptions) EncodeDecoderOptions {
	o := *opts
	o.decOpts = value
	return &o
}

func (opts *encdecOptions) EncodeDecoderPool() EncodeDecoderPool {
	return opts.pool
}

func (opts *encdecOptions) SetEncodeDecoderPool(pool EncodeDecoderPool) EncodeDecoderOptions {
	o := *opts
	o.pool = pool
	return &o
}
