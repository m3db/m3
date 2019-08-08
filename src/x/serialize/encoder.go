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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
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
	buf            []byte
	checkedBytes   checked.Bytes
	bufUInt16Const [2]byte
	bufUInt16      []byte

	headerWritten bool

	maxTagLiteralLength int
	maxNumberTags       int
	pool                TagEncoderPool
}

func newTagEncoder(
	newFn newCheckedBytesFn,
	opts TagEncoderOptions,
	pool TagEncoderPool,
) TagEncoder {
	b := make([]byte, 0, opts.InitialCapacity())
	cb := newFn(nil, nil)
	e := &encoder{
		buf:                 b,
		checkedBytes:        cb,
		maxTagLiteralLength: int(opts.TagSerializationLimits().MaxTagLiteralLength()),
		maxNumberTags:       int(opts.TagSerializationLimits().MaxNumberTags()),
		pool:                pool,
	}
	e.bufUInt16 = e.bufUInt16Const[:]
	return e
}

func (e *encoder) Encode(tagsIter ident.TagIterator) error {
	numTags := tagsIter.Remaining()
	if err := e.writeHeader(numTags); err != nil {
		return err
	}

	if sliceIter, ok := tagsIter.(interface {
		SliceIter() []ident.Tag
	}); ok {
		// Optimized pathway for interfaces that support it.
		tagsSlice := sliceIter.SliceIter()
		if len(tagsSlice) != numTags {
			e.buf = e.buf[:0]
			return fmt.Errorf("iterator returned %d tags but expected %d",
				len(tagsSlice), numTags)
		}

		for _, tag := range tagsSlice {
			if err := e.encodeTag(tag.Name.Bytes(), tag.Value.Bytes()); err != nil {
				e.buf = e.buf[:0]
				return err
			}
		}
	} else {
		encoded := 0
		tagsIter.Restart()
		for tagsIter.Next() {
			tag := tagsIter.Current()
			err := e.encodeTag(tag.Name.Bytes(), tag.Value.Bytes())
			if err != nil {
				tagsIter.Restart()
				e.buf = e.buf[:0]
				return err
			}
			encoded++
		}

		if err := tagsIter.Err(); err != nil {
			tagsIter.Restart()
			e.buf = e.buf[:0]
			return err
		}

		if encoded != numTags {
			tagsIter.Restart()
			e.buf = e.buf[:0]
			return fmt.Errorf("iterator returned %d tags but expected %d",
				encoded, numTags)
		}
	}

	e.checkedBytes.IncRef()
	e.checkedBytes.Reset(e.buf)

	return nil
}

func (e *encoder) writeHeader(numTags int) error {
	if e.checkedBytes.NumRef() > 0 {
		return errTagEncoderInUse
	}

	if numTags > e.maxNumberTags {
		return fmt.Errorf(
			"too many tags to encode (%d), limit is: %d",
			numTags, e.maxNumberTags)
	}

	e.buf = append(e.buf, headerMagicBytes...)
	numTagsBytes := encodeUInt16Buf(e.bufUInt16, uint16(numTags))
	e.buf = append(e.buf, numTagsBytes...)

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
	e.buf = e.buf[:0]
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

func (e *encoder) encodeTag(name, value []byte) error {
	if len(name) == 0 {
		return errEmptyTagNameLiteral
	}
	if len(name) >= e.maxTagLiteralLength {
		return errTagLiteralTooLong
	}
	if len(value) >= e.maxTagLiteralLength {
		return errTagLiteralTooLong
	}

	ld := uint16(len(name))
	byteOrder.PutUint16(e.bufUInt16, ld)
	e.buf = append(e.buf, e.bufUInt16...)
	e.buf = append(e.buf, name...)

	ld = uint16(len(value))
	byteOrder.PutUint16(e.bufUInt16, ld)
	e.buf = append(e.buf, e.bufUInt16...)
	e.buf = append(e.buf, value...)

	return nil
}

func encodeUInt16Buf(bytes []byte, v uint16) []byte {
	byteOrder.PutUint16(bytes, v)
	return bytes
}

func encodeUInt16(v uint16) []byte {
	// NB(r): Using a [2]byte on the stack here was still escaping.
	return encodeUInt16Buf(make([]byte, 2), v)
}

func decodeUInt16(b []byte) uint16 {
	return byteOrder.Uint16(b)
}
