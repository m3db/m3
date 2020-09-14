// Copyright (c) 2016 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

import (
	"hash"
	"hash/adler32"
)

// DecodingOptions provide a set of options for decoding data.
type DecodingOptions interface {
	// SetAllocDecodedBytes sets whether we allocate new space when decoding
	// a byte slice.
	SetAllocDecodedBytes(value bool) DecodingOptions

	// AllocDecodedBytes determines whether we allocate new space when decoding
	// a byte slice.
	AllocDecodedBytes() bool

	// SetHash32 sets the hash32 method for this decoder.
	SetHash32(value hash.Hash32) DecodingOptions

	// SetHash sets the hash method for this decoder.
	Hash32() hash.Hash32
}

const (
	defaultAllocDecodedBytes = false
)

var (
	defaultHash = adler32.New()
)

type decodingOptions struct {
	allocDecodedBytes bool
	hash32            hash.Hash32
}

// NewDecodingOptions creates a new set of decoding options.
func NewDecodingOptions() DecodingOptions {
	return &decodingOptions{
		allocDecodedBytes: defaultAllocDecodedBytes,
		hash32:            defaultHash,
	}
}

func (o *decodingOptions) SetAllocDecodedBytes(value bool) DecodingOptions {
	opts := *o
	opts.allocDecodedBytes = value
	return &opts
}

func (o *decodingOptions) AllocDecodedBytes() bool {
	return o.allocDecodedBytes
}

func (o *decodingOptions) SetHash32(value hash.Hash32) DecodingOptions {
	opts := *o
	opts.hash32 = value
	return &opts
}

func (o *decodingOptions) Hash32() hash.Hash32 {
	return o.hash32
}
