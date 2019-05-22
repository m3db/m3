// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Softwarw.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package docs

import (
	"github.com/golang/snappy"
)

func newSnappyBytesCompressor(initialSize int) Compressor {
	return &snappyBytesCompressor{
		bytes: make([]byte, initialSize),
	}
}

type snappyBytesCompressor struct {
	bytes []byte
}

func (l *snappyBytesCompressor) Reset() { l.bytes = l.bytes[:0] }

func (l *snappyBytesCompressor) Compress(b []byte) ([]byte, error) {
	max := snappy.MaxEncodedLen(len(b))
	if cap(l.bytes) < max {
		l.bytes = make([]byte, max)
	}

	l.bytes = l.bytes[:max]
	l.bytes = snappy.Encode(l.bytes, b)
	return l.bytes, nil
}

func newSnappyBytesDecompressor(copts CompressionOptions) Decompressor {
	return &snappyBytesDecompressor{
		bytes: make([]byte, copts.PageSize),
		copts: copts,
	}
}

type snappyBytesDecompressor struct {
	bytes []byte
	copts CompressionOptions
}

func (l *snappyBytesDecompressor) CompressionOptions() CompressionOptions { return l.copts }
func (l *snappyBytesDecompressor) Reset()                                 { l.bytes = l.bytes[:0] }

func (l *snappyBytesDecompressor) Decompress(b []byte) ([]byte, error) {
	max, err := snappy.DecodedLen(b)
	if err != nil {
		return nil, err
	}

	if cap(l.bytes) < max {
		l.bytes = make([]byte, max)
	}
	l.bytes = l.bytes[:max]

	l.bytes, err = snappy.Decode(l.bytes, b)
	if err != nil {
		return nil, err
	}

	return l.bytes, nil
}
