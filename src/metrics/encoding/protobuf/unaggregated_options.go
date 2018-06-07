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

package protobuf

import "github.com/m3db/m3x/pool"

const (
	defaultInitBufferSize = 2880

	// The maximum supported unaggregated message size is 50MB by default.
	// This is to protect the encoder and the decoder from incurring signicant
	// memory overhead or even OOMing due to excessively large payload or
	// network packet corruption.
	defaultMaxUnaggregatedMessageSize = 50 * 1024 * 1024
)

// UnaggregatedOptions provide a set of options for the unaggregated encoder and iterator.
type UnaggregatedOptions interface {
	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) UnaggregatedOptions

	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetInitBufferSize sets the initial buffer size.
	SetInitBufferSize(value int) UnaggregatedOptions

	// InitBufferSize returns the initial buffer size.
	InitBufferSize() int

	// SetMaxMessageSize sets the maximum message size.
	SetMaxMessageSize(value int) UnaggregatedOptions

	// MaxMessageSize returns the maximum message size.
	MaxMessageSize() int
}

type unaggregatedOptions struct {
	bytesPool      pool.BytesPool
	initBufferSize int
	maxMessageSize int
}

// NewUnaggregatedOptions create a new set of unaggregated options.
func NewUnaggregatedOptions() UnaggregatedOptions {
	p := pool.NewBytesPool(nil, nil)
	p.Init()
	return &unaggregatedOptions{
		bytesPool:      p,
		initBufferSize: defaultInitBufferSize,
		maxMessageSize: defaultMaxUnaggregatedMessageSize,
	}
}

func (o *unaggregatedOptions) SetBytesPool(value pool.BytesPool) UnaggregatedOptions {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *unaggregatedOptions) BytesPool() pool.BytesPool {
	return o.bytesPool
}

func (o *unaggregatedOptions) SetInitBufferSize(value int) UnaggregatedOptions {
	opts := *o
	opts.initBufferSize = value
	return &opts
}

func (o *unaggregatedOptions) InitBufferSize() int {
	return o.initBufferSize
}

func (o *unaggregatedOptions) SetMaxMessageSize(value int) UnaggregatedOptions {
	opts := *o
	opts.maxMessageSize = value
	return &opts
}

func (o *unaggregatedOptions) MaxMessageSize() int {
	return o.maxMessageSize
}
