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
	"math"

	"github.com/golang/protobuf/proto"
)

type customFieldMarshaler interface {
	encFloat64(tag int32, x float64)
	encFloat32(tag int32, x float32)
	encInt32(tag int32, x int32)
	encSInt32(tag int32, x int32)
	encSFixedInt32(tax int32, x int32)
	encUInt32(tag int32, x uint32)
	encInt64(tag int32, x int64)
	encSInt64(tag int32, x int64)
	encSFixedInt64(tax int32, x int64)
	encUInt64(tag int32, x uint64)
	encBool(tag int32, x bool)
	encBytes(tag int32, x []byte)
	encPartialProto(tag int32, x []byte)
	bytes() []byte
	setBytes(b []byte)
	reset()
}

type customMarshaler struct {
	buf *buffer
}

func newCustomMarshaler() customFieldMarshaler {
	return &customMarshaler{
		buf: newCodedBuffer(nil),
	}
}

func (m *customMarshaler) encFloat64(tag int32, x float64) {
	if x == 0 {
		// Default values are not included in the stream.
		return
	}

	m.buf.encodeTagAndWireType(tag, proto.WireFixed64)
	m.buf.encodeFixed64(math.Float64bits(x))
}

func (m *customMarshaler) encFloat32(tag int32, x float32) {
	if x == 0 {
		// Default values are not included in the stream.
		return
	}

	m.buf.encodeTagAndWireType(tag, proto.WireFixed32)
	m.buf.encodeFixed32(math.Float32bits(x))
}

func (m *customMarshaler) encBool(tag int32, x bool) {
	if !x {
		// Default values are not included in the stream.
		return
	}

	m.encUInt64(tag, 1)
}

func (m *customMarshaler) encInt32(tag int32, x int32) {
	m.encUInt64(tag, uint64(x))
}

func (m *customMarshaler) encSInt32(tag int32, x int32) {
	m.encUInt64(tag, encodeZigZag32(x))
}

func (m *customMarshaler) encSFixedInt32(tag int32, x int32) {
	m.buf.encodeTagAndWireType(tag, proto.WireFixed32)
	m.buf.encodeFixed32(uint32(x))
}

func (m *customMarshaler) encUInt32(tag int32, x uint32) {
	m.encUInt64(tag, uint64(x))
}

func (m *customMarshaler) encInt64(tag int32, x int64) {
	m.encUInt64(tag, uint64(x))
}

func (m *customMarshaler) encSInt64(tag int32, x int64) {
	m.encUInt64(tag, encodeZigZag64(x))
}

func (m *customMarshaler) encSFixedInt64(tag int32, x int64) {
	m.buf.encodeTagAndWireType(tag, proto.WireFixed64)
	m.buf.encodeFixed64(uint64(x))
}

func (m *customMarshaler) encUInt64(tag int32, x uint64) {
	if x == 0 {
		// Default values are not included in the stream.
		return
	}

	m.buf.encodeTagAndWireType(tag, proto.WireVarint)
	m.buf.encodeVarint(x)
}

func (m *customMarshaler) encBytes(tag int32, x []byte) {
	if len(x) == 0 {
		// Default values are not included in the stream.
		return
	}

	m.buf.encodeTagAndWireType(tag, proto.WireBytes)
	m.buf.encodeRawBytes(x)
}

func (m *customMarshaler) encPartialProto(tag int32, x []byte) {
	m.buf.append(x)
}

func (m *customMarshaler) bytes() []byte {
	return m.buf.buf
}

func (m *customMarshaler) setBytes(b []byte) {
	m.buf.buf = b
}

func (m *customMarshaler) reset() {
	b := m.buf.buf
	if b != nil {
		b = b[:0]
	}
	m.buf.reset(b)
}
