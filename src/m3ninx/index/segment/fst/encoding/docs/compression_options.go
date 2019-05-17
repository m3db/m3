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

import "fmt"

func (co CompressionOptions) Equal(other CompressionOptions) bool {
	return co.Type == other.Type && co.PageSize == other.PageSize
}

func (co CompressionOptions) newCompressor() (Compressor, error) {
	switch co.Type {
	case SnappyCompressionType:
		return newSnappyBytesCompressor(co.PageSize), nil
	case DeflateCompressionType:
		return newDeflateBytesCompressor(co.PageSize)
	case LZ4CompressionType:
		return newLZ4BytesCompressor(co.PageSize)
	}
	return nil, fmt.Errorf("unknown compression type: %v", co.Type)
}

func (co CompressionOptions) newDecompressor() (Decompressor, error) {
	switch co.Type {
	case SnappyCompressionType:
		return newSnappyBytesDecompressor(co), nil
	case DeflateCompressionType:
		return newDeflateBytesDecompressor(co), nil
	case LZ4CompressionType:
		return newLZ4BytesDecompressor(co), nil
	}
	return nil, fmt.Errorf("unknown compression type: %v", co.Type)
}
