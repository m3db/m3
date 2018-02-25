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

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/m3db/m3x/ident"
)

/*
 * Serialization scheme to combat Thrift's allocation hell.
 *
 * Given Tags (i.e. key-values) this allows the bijective serialization to,
 * and from Tags <--> []byte.
 *
 * Consider example, Tags: {"abc": "defg", "x": "foo"}
 * this translates to:
 * []byte(
 *    MAGIC_MARKER + NUMBER_TAGS
 *                 + LENGTH([]byte("abc"))  + []byte("abc")
 *                 + LENGTH([]byte("defg")) + []byte("abc")
 *                 + LENGTH([]byte("x"))    + []byte("x")
 *                 + LENGTH([]byte("foo"))  + []byte("foo")
 * )
 *
 * Where MAGIC_MARKER/NUMBER_TAGS/LENGTH are maximum 2 bytes.
 */

var (
	initialBufferLength                  = 1 << 12
	byteOrder           binary.ByteOrder = binary.LittleEndian
	headerMagicBytes                     = encodeUInt16(HeaderMagicNumber)
)

var (
	errEncoderInUse      = errors.New("encoder already in use")
	errTagLiteralTooLong = errors.New("literal is too long")
)

type encoder struct {
	inuse bool
	buf   *bytes.Buffer

	pool EncoderPool
}

func newEncoder(initialSize int, pool EncoderPool) Encoder {
	b := make([]byte, 0, initialSize)
	return &encoder{
		buf:  bytes.NewBuffer(b),
		pool: pool,
	}
}

func (e *encoder) Encode(srcTags ident.TagIterator) error {
	if e.inuse {
		return errEncoderInUse
	}

	tags := srcTags.Clone()
	defer tags.Close()

	numTags := tags.Remaining()
	if numTags > int(MaxNumberTags) {
		return fmt.Errorf("too many tags to encode (%d), limit is: %d", numTags, MaxNumberTags)
	}

	e.inuse = true
	if _, err := e.buf.Write(headerMagicBytes); err != nil {
		return err
	}

	if _, err := e.buf.Write(encodeUInt16(uint16(numTags))); err != nil {
		return err
	}

	for tags.Next() {
		tag := tags.Current()
		if err := e.encodeTag(tag); err != nil {
			return err
		}
	}

	return tags.Err()
}

func (e *encoder) Data() []byte { return e.buf.Bytes() }

func (e *encoder) Reset() {
	e.buf.Reset()
	e.inuse = false
}

func (e *encoder) Finalize() {
	e.Reset()
	p := e.pool
	if p == nil {
		return
	}
	p.Put(e)
}

func (e *encoder) encodeTag(t ident.Tag) error {
	if err := e.encodeID(t.Name); err != nil {
		return err
	}

	return e.encodeID(t.Value)
}

func (e *encoder) encodeID(i ident.ID) error {
	d := i.Data().Get()
	if len(d) >= int(MaxTagLiteralLength) {
		return errTagLiteralTooLong
	}

	ld := uint16(len(d))
	if _, err := e.buf.Write(encodeUInt16(ld)); err != nil {
		return err
	}

	if _, err := e.buf.Write(d); err != nil {
		return err
	}

	return nil
}

func encodeUInt16(v uint16) []byte {
	var bytes [2]byte
	byteOrder.PutUint16(bytes[:], v)
	return bytes[:]
}

func decodeUInt16(b []byte) uint16 {
	return byteOrder.Uint16(b)
}
