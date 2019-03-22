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

package pickle

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
)

var (
	programStart = []uint8{opProto, 0x2}
	programEnd   = []uint8{opStop}
	listStart    = []uint8{opEmptyList, opMark}
	dictStart    = []uint8{opEmptyDict, opMark}
)

// A Writer is capable writing out the opcodes required by the pickle format.
// Note that this is a very limited implementation of pickling; just enough for
// us to implement the opcodes required by graphite /render.
type Writer struct {
	w   *bufio.Writer
	buf [8]byte
	err error
}

// NewWriter creates a new pickle writer.
func NewWriter(w io.Writer) *Writer {
	pw := &Writer{
		w: bufio.NewWriter(w),
	}

	_, pw.err = pw.w.Write(programStart)
	return pw
}

// BeginDict starts marshalling a python dict.
func (p *Writer) BeginDict() {
	if p.err != nil {
		return
	}

	if _, p.err = p.w.Write(dictStart); p.err != nil {
		return
	}
}

// WriteDictKey writes a dictionary key.
func (p *Writer) WriteDictKey(s string) {
	p.WriteString(s)
}

// EndDict ends marshalling a python dict.
func (p *Writer) EndDict() {
	if p.err != nil {
		return
	}

	p.err = p.w.WriteByte(opSetItems)
}

// BeginList begins writing a new python list.
func (p *Writer) BeginList() {
	if p.err != nil {
		return
	}

	_, p.err = p.w.Write(listStart)
}

// EndList ends writing a python list.
func (p *Writer) EndList() {
	if p.err != nil {
		return
	}

	p.w.WriteByte(opAppends)
}

// WriteNone writes a python `None`.
func (p *Writer) WriteNone() {
	if p.err != nil {
		return
	}

	p.err = p.w.WriteByte(opNone)
}

// WriteFloat64 writes a float64 value. NaNs are converted in `None`.
func (p *Writer) WriteFloat64(v float64) {
	if math.IsNaN(v) {
		p.WriteNone()
		return
	}

	if p.err != nil {
		return
	}

	if p.err = p.w.WriteByte(opBinFloat); p.err != nil {
		return
	}

	binary.BigEndian.PutUint64(p.buf[:], math.Float64bits(v))
	_, p.err = p.w.Write(p.buf[:])
}

// WriteString writes a python string.
func (p *Writer) WriteString(s string) {
	if p.err != nil {
		return
	}

	if p.err = p.w.WriteByte(opBinUnicode); p.err != nil {
		return
	}

	binary.LittleEndian.PutUint32(p.buf[:4], uint32(len(s)))
	if _, p.err = p.w.Write(p.buf[:4]); p.err != nil {
		return
	}

	_, p.err = p.w.WriteString(s)
}

// WriteInt writes an int value.
func (p *Writer) WriteInt(n int) {
	if p.err != nil {
		return
	}

	if p.err = p.w.WriteByte(opBinInt); p.err != nil {
		return
	}

	binary.LittleEndian.PutUint32(p.buf[:4], uint32(n))
	_, p.err = p.w.Write(p.buf[:4])
}

// Close closes the writer, marking the end of the stream and flushing any
// pending values.
func (p *Writer) Close() error {
	if p.err != nil {
		return p.err
	}

	if _, p.err = p.w.Write(programEnd); p.err != nil {
		return p.err
	}

	if p.err = p.w.Flush(); p.err != nil {
		return p.err
	}

	return nil
}
