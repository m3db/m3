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
	buf            *bytes.Buffer
	checkedBytes   checked.Bytes
	bufUInt16Const [2]byte
	bufUInt16      []byte

	headerWritten bool

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
	e := &encoder{
		buf:          bytes.NewBuffer(b),
		checkedBytes: cb,
		opts:         opts,
		pool:         pool,
	}
	e.bufUInt16 = e.bufUInt16Const[:]
	return e
}

func (e *encoder) Encode(tags ident.TagIterator) error {
	tags.Restart()
	defer tags.Restart()

	numTags := tags.Remaining()
	if err := e.writeHeader(numTags); err != nil {
		return err
	}

	encoded := 0
	for tags.Next() {
		tag := tags.Current()
		err := e.encodeTag(tag.Name.Bytes(), tag.Value.Bytes())
		if err != nil {
			e.buf.Reset()
			return err
		}
		encoded++
	}
	if encoded != numTags {
		e.buf.Reset()
		return fmt.Errorf("iterator returned %d tags but expected %d",
			encoded, numTags)
	}

	if err := tags.Err(); err != nil {
		e.buf.Reset()
		return err
	}

	e.checkedBytes.IncRef()
	e.checkedBytes.Reset(e.buf.Bytes())

	return nil
}

func (e *encoder) writeHeader(numTags int) error {
	if e.checkedBytes.NumRef() > 0 {
		return errTagEncoderInUse
	}

	max := int(e.opts.TagSerializationLimits().MaxNumberTags())
	if numTags > max {
		return fmt.Errorf("too many tags to encode (%d), limit is: %d", numTags, max)
	}

	if _, err := e.buf.Write(headerMagicBytes); err != nil {
		e.buf.Reset()
		return err
	}

	numTagsBytes := encodeUInt16Buf(e.bufUInt16, uint16(numTags))
	if _, err := e.buf.Write(numTagsBytes); err != nil {
		e.buf.Reset()
		return err
	}

	return nil
}

func (e *encoder) EncodeMetricTags(
	tags MetricTagsIterator,
	opts EncodeMetricTagsOptions,
) error {
	numTags := tags.NumTags()
	for _, append := range opts.AppendTags.Values() {
		_, exists := tags.TagValue(append.Name.Bytes())
		if !exists {
			numTags++
		}
	}
	for _, exclude := range opts.ExcludeTags {
		_, exists := tags.TagValue(exclude.Bytes())
		if exists {
			numTags--
		}
	}

	if err := e.writeHeader(numTags); err != nil {
		return err
	}

	appendTags := opts.AppendTags.Values()
	appendTagIdx := 0

	for tags.Next() {
		name, value := tags.Current()

		skip := false
		if appendTagIdx < len(appendTags) {
			// Need to check if need to append tag.
			append := appendTags[appendTagIdx]
			cmp := bytes.Compare(append.Name.Bytes(), name)
			if cmp <= 0 {
				if cmp == 0 {
					// Skip this after we encode the append tag which overrides
					// the current encoded tag.
					skip = true
				}
				err := e.encodeTag(append.Name.Bytes(), append.Value.Bytes())
				if err != nil {
					e.buf.Reset()
					return err
				}
			}
		}

		for _, exclude := range opts.ExcludeTags {
			if bytes.Equal(name, exclude.Bytes()) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		if err := e.encodeTag(name, value); err != nil {
			e.buf.Reset()
			return err
		}
	}

	// Append remaining tags.
	for i := appendTagIdx; i < len(appendTags); i++ {
		append := appendTags[appendTagIdx]
		err := e.encodeTag(append.Name.Bytes(), append.Value.Bytes())
		if err != nil {
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

func (e *encoder) encodeTag(name, value []byte) error {
	if len(name) == 0 {
		return errEmptyTagNameLiteral
	}

	if err := e.encodeID(name); err != nil {
		return err
	}

	return e.encodeID(value)
}

func (e *encoder) encodeID(d []byte) error {
	max := int(e.opts.TagSerializationLimits().MaxTagLiteralLength())
	if len(d) >= max {
		return errTagLiteralTooLong
	}

	ld := uint16(len(d))
	ldBytes := encodeUInt16Buf(e.bufUInt16, ld)
	if _, err := e.buf.Write(ldBytes); err != nil {
		return err
	}

	if _, err := e.buf.Write(d); err != nil {
		return err
	}

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
