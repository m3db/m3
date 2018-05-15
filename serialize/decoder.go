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
	"errors"
	"fmt"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
)

var (
	errIncorrectHeader               = errors.New("header magic number does not match expected value")
	errInvalidByteStreamIDDecoding   = errors.New("internal error, invalid byte stream while decoding ID")
	errInvalidByteStreamUintDecoding = errors.New("internal error, invalid byte stream while decoding uint")
)

type decoder struct {
	checkedData checked.Bytes
	data        []byte
	remaining   int
	err         error
	hasCurrent  bool
	current     ident.Tag

	opts TagDecoderOptions
	pool TagDecoderPool
}

func newTagDecoder(opts TagDecoderOptions, pool TagDecoderPool) TagDecoder {
	return &decoder{
		opts: opts,
		pool: pool,
	}
}

func (d *decoder) Reset(b checked.Bytes) {
	d.resetForReuse()
	d.checkedData = b
	d.checkedData.IncRef()
	d.data = d.checkedData.Bytes()

	header, err := d.decodeUInt16()
	if err != nil {
		d.err = err
		return
	}

	if header != headerMagicNumber {
		d.err = errIncorrectHeader
		return
	}

	remain, err := d.decodeUInt16()
	if err != nil {
		d.err = err
		return
	}

	if limit := d.opts.TagSerializationLimits().MaxNumberTags(); remain > limit {
		d.err = fmt.Errorf("too many tags [ limit = %d, observed = %d ]", limit, remain)
		return
	}

	d.remaining = int(remain)
}

func (d *decoder) Next() bool {
	d.releaseCurrent()
	if d.err != nil || d.remaining <= 0 {
		return false
	}

	t, err := d.decodeTag()
	if err != nil {
		d.err = err
		return false
	}

	d.remaining--
	d.current = t
	d.hasCurrent = true
	return true
}

func (d *decoder) Current() ident.Tag {
	return d.current
}

func (d *decoder) decodeTag() (ident.Tag, error) {
	name, err := d.decodeID()
	if err != nil {
		return ident.Tag{}, err
	}

	value, err := d.decodeID()
	if err != nil {
		name.Finalize()
		return ident.Tag{}, err
	}

	return ident.Tag{Name: name, Value: value}, nil
}

func (d *decoder) decodeID() (ident.ID, error) {
	l, err := d.decodeUInt16()
	if err != nil {
		return nil, err
	}

	if limit := d.opts.TagSerializationLimits().MaxTagLiteralLength(); l > limit {
		return nil, fmt.Errorf("tag literal too long [ limit = %d, observed = %d ]", limit, int(l))
	}

	if len(d.data) < int(l) {
		return nil, errInvalidByteStreamIDDecoding
	}

	// incRef to indicate another checked.Bytes has a
	// reference to the original bytes
	d.checkedData.IncRef()
	b := d.opts.CheckedBytesWrapperPool().Get(d.data[:l])
	d.data = d.data[l:]

	return ident.BinaryID(b), nil
}

func (d *decoder) decodeUInt16() (uint16, error) {
	if len(d.data) < 2 {
		return 0, errInvalidByteStreamUintDecoding
	}

	n := decodeUInt16(d.data)
	d.data = d.data[2:]
	return n, nil
}

func (d *decoder) Err() error {
	return d.err
}

func (d *decoder) releaseCurrent() {
	if n := d.current.Name; n != nil {
		n.Finalize()
		d.checkedData.DecRef() // indicate we've released the extra ref
		d.current.Name = nil
	}
	if v := d.current.Value; v != nil {
		v.Finalize()
		d.checkedData.DecRef() // indicate we've released the extra ref
		d.current.Value = nil
	}
	d.hasCurrent = false
}

func (d *decoder) Remaining() int {
	return d.remaining
}

func (d *decoder) resetForReuse() {
	d.releaseCurrent()
	d.data = nil
	d.err = nil
	d.remaining = 0
	if d.checkedData == nil {
		return
	}
	d.checkedData.DecRef()
	if d.checkedData.NumRef() == 0 {
		d.checkedData.Finalize()
	}
	d.checkedData = nil
}

func (d *decoder) Close() {
	d.resetForReuse()
	if d.pool == nil {
		return
	}
	d.pool.Put(d)
}

func (d *decoder) cloneCurrent() ident.Tag {
	name := d.opts.CheckedBytesWrapperPool().Get(d.current.Name.Bytes())
	d.checkedData.IncRef()
	value := d.opts.CheckedBytesWrapperPool().Get(d.current.Value.Bytes())
	d.checkedData.IncRef()
	return ident.BinaryTag(name, value)
}

func (d *decoder) Duplicate() ident.TagIterator {
	copy := *d
	if copy.hasCurrent {
		copy.current = d.cloneCurrent()
	}
	if copy.checkedData != nil {
		copy.checkedData.IncRef()
	}
	return &copy
}
