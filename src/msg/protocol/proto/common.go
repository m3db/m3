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
	"encoding/binary"

	"github.com/m3db/m3x/pool"
)

const (
	sizeBufferSize = 4
)

var sizeEncodeDecoder = binary.BigEndian

func growDataBufferIfNeeded(buffer []byte, targetSize int, pool pool.BytesPool) []byte {
	if len(buffer) >= targetSize {
		return buffer
	}
	if pool != nil {
		pool.Put(buffer)
	}
	return getByteSliceWithLength(targetSize, pool)
}

func getByteSliceWithLength(targetSize int, pool pool.BytesPool) []byte {
	if pool == nil {
		return make([]byte, targetSize)
	}
	b := pool.Get(targetSize)
	// Make sure there is enough length in the slice.
	return b[:cap(b)]
}
