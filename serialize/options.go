// Copyright (c) 2016 Uber Technologies, Inc.
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

package serialize

import "math"

var (
	// defaultMaxNumberTags is the maximum number of tags that can be encoded.
	defaultMaxNumberTags uint16 = math.MaxUint16

	// defaultMaxTagLiteralLength is the maximum length of a tag Name/Value.
	defaultMaxTagLiteralLength uint16 = 4096

	// defaultInitialCapacity is the default initial capacity of the bytes
	// underlying the encoder.
	defaultInitialCapacity = 1024
)

type encodeOpts struct {
	initialCapacity     int
	maxNumberTags       uint16
	maxTagLiteralLength uint16
}

func NewTagEncoderOptions() TagEncoderOptions {
	return &encodeOpts{
		initialCapacity:     defaultInitialCapacity,
		maxNumberTags:       defaultMaxNumberTags,
		maxTagLiteralLength: defaultMaxTagLiteralLength,
	}
}

func (o *encodeOpts) SetInitialCapacity(v int) TagEncoderOptions {
	opts := *o
	opts.initialCapacity = v
	return &opts
}

func (o *encodeOpts) InitialCapacity() int {
	return o.initialCapacity
}

func (o *encodeOpts) SetMaxNumberTags(v uint16) TagEncoderOptions {
	opts := *o
	opts.maxNumberTags = v
	return &opts
}

func (o *encodeOpts) MaxNumberTags() uint16 {
	return o.maxNumberTags
}

func (o *encodeOpts) SetMaxTagLiteralLength(v uint16) TagEncoderOptions {
	opts := *o
	opts.maxTagLiteralLength = v
	return &opts
}

func (o *encodeOpts) MaxTagLiteralLength() uint16 {
	return o.maxTagLiteralLength
}
