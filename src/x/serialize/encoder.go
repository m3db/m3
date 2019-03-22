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

package serialize

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
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
	byteOrder        binary.ByteOrder = binary.LittleEndian
	headerMagicBytes                  = encodeUInt16(headerMagicNumber)
)

var (
	errTagEncoderInUse     = errors.New("encoder already in use")
	errTagLiteralTooLong   = errors.New("literal is too long")
	errEmptyTagNameLiteral = xerrors.NewInvalidParamsError(errors.New("tag name cannot be empty"))
)

type newCheckedBytesFn func([]byte, checked.BytesOptions) checked.Bytes

var defaultNewCheckedBytesFn = checked.NewBytes

type encoder struct {
	buf          *bytes.Buffer
	checkedBytes checked.Bytes

	opts TagEncoderOptions
	pool TagEncoderPool
}

func newTagEncoder(
	newFn newCheckedBytesFn,
	opts TagEncoderOptions,
	pool TagEncoderPool,
) TagEncoder {
	b := make([]byte, 0, opts.InitialCapacity())
	cb := newFn(nil, nil)
	return &encoder{
		buf:          bytes.NewBuffer(b),
		checkedBytes: cb,
		opts:         opts,
		pool:         pool,
	}
}

func (e *encoder) Encode(srcTags ident.TagIterator) error {
	if e.checkedBytes.NumRef() > 0 {
		return errTagEncoderInUse
	}

	tags := srcTags.Duplicate()
	defer tags.Close()

	numTags := tags.Remaining()
	max := int(e.opts.TagSerializationLimits().MaxNumberTags())
	if numTags > max {
		return fmt.Errorf("too many tags to encode (%d), limit is: %d", numTags, max)
	}

	if _, err := e.buf.Write(headerMagicBytes); err != nil {
		e.buf.Reset()
		return err
	}

	if _, err := e.buf.Write(encodeUInt16(uint16(numTags))); err != nil {
		e.buf.Reset()
		return err
	}

	for tags.Next() {
		tag := tags.Current()
		if err := e.encodeTag(tag); err != nil {
			e.buf.Reset()
			return err
		}
	}

	if err := tags.Err(); err != nil {
		e.buf.Reset()
		return err
	}

	e.checkedBytes.IncRef()
	e.checkedBytes.Reset(e.buf.Bytes())

	return nil
}

func (e *encoder) Data() (checked.Bytes, bool) {
	if e.checkedBytes.NumRef() == 0 {
		return nil, false
	}
	return e.checkedBytes, true
}

func (e *encoder) Reset() {
	if e.checkedBytes.NumRef() == 0 {
		return
	}
	e.buf.Reset()
	e.checkedBytes.Reset(nil)
	e.checkedBytes.DecRef()
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
	if len(t.Name.Bytes()) == 0 {
		return errEmptyTagNameLiteral
	}

	if err := e.encodeID(t.Name); err != nil {
		return err
	}

	return e.encodeID(t.Value)
}

func (e *encoder) encodeID(i ident.ID) error {
	d := i.Bytes()

	max := int(e.opts.TagSerializationLimits().MaxTagLiteralLength())
	if len(d) >= max {
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
