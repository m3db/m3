// Copyright (c) 2021 Uber Technologies, Inc.
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
	"fmt"
)

// uncheckedDecoder is a fast decoder that skips all safety checks (i.e. ref counts).
// it is suitable to use this when you are confident the provided bytes to decode are not shared.
type uncheckedDecoder struct {
	// immutable state for the lifetime of the decoder
	maxTags      uint16
	maxTagLength uint16

	// immutable state for the lifetime of a single use.
	length int

	// mutable state during a single use.
	data            []byte
	remaining       int
	err             error
	currentTagName  []byte
	currentTagValue []byte
}

func newUncheckedTagDecoder(tagLimits TagSerializationLimits) *uncheckedDecoder {
	return &uncheckedDecoder{
		// inline the config values since this call is in the hotpath of the matcher. saved about 1% cpu.
		maxTags:      tagLimits.MaxNumberTags(),
		maxTagLength: tagLimits.MaxTagLiteralLength(),
	}
}

func (d *uncheckedDecoder) reset(b []byte) {
	d.data = b
	var length uint16
	if len(d.data) == 0 {
		d.length = 0
		d.remaining = 0
	} else {
		header, err := d.decodeUInt16()
		if err != nil {
			d.err = err
			return
		}

		if header != HeaderMagicNumber {
			d.err = ErrIncorrectHeader
			return
		}

		length, err = d.decodeUInt16()
		if err != nil {
			d.err = err
			return
		}

		if limit := d.maxTags; length > limit {
			d.err = fmt.Errorf("too many tags [ limit = %d, observed = %d ]", limit, length)
			return
		}
	}
	d.length = int(length)
	d.remaining = int(length)
	d.err = nil
	d.currentTagName = nil
	d.currentTagValue = nil
}

func (d *uncheckedDecoder) next() bool {
	if d.err != nil || d.remaining <= 0 {
		return false
	}

	if err := d.decodeTag(); err != nil {
		d.err = err
		return false
	}

	d.remaining--
	return true
}

func (d *uncheckedDecoder) current() ([]byte, []byte) {
	return d.currentTagName, d.currentTagValue
}

func (d *uncheckedDecoder) decodeTag() error {
	var err error
	d.currentTagName, err = d.decodeID()
	if err != nil {
		return err
	}
	if len(d.currentTagName) == 0 {
		return ErrEmptyTagNameLiteral
	}

	d.currentTagValue, err = d.decodeID()
	if err != nil {
		return err
	}
	return nil
}

func (d *uncheckedDecoder) decodeID() ([]byte, error) {
	l, err := d.decodeUInt16()
	if err != nil {
		return nil, err
	}

	if limit := d.maxTagLength; l > limit {
		return nil, fmt.Errorf("tag literal too long [ limit = %d, observed = %d ]", limit, int(l))
	}

	if len(d.data) < int(l) {
		return nil, errInvalidByteStreamIDDecoding
	}

	id := d.data[:l]
	d.data = d.data[l:]

	return id, nil
}

func (d *uncheckedDecoder) decodeUInt16() (uint16, error) {
	if len(d.data) < 2 {
		return 0, errInvalidByteStreamUintDecoding
	}

	n := decodeUInt16(d.data)
	d.data = d.data[2:]
	return n, nil
}
