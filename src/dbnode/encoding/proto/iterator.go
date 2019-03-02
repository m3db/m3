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
	"io"
	"math"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
)

type iterator struct {
	err              error
	schema           *desc.MessageDescriptor
	stream           encoding.IStream
	opts             encoding.Options
	consumedFirstTSZ bool
	lastIterated     *dynamic.Message
	tszFields        []tszFieldState
}

// NewIterator creates a new iterator.
// TODO: Make sure b and schema not nil.
func NewIterator(
	reader io.Reader,
	schema *desc.MessageDescriptor,
	opts encoding.Options,
) (*iterator, error) {
	iter := &iterator{
		schema: schema,
		stream: encoding.NewIStream(reader),
		opts:   opts,
		// TODO: These need to be possibly updated as we traverse a stream
		tszFields: tszFields(nil, schema),
	}

	return iter, nil
}

func (it *iterator) Next() bool {
	if !it.hasNext() {
		return false
	}

	if !it.consumedFirstTSZ {
		it.readFirstTSZValues()
	} else {
		it.readNextTSZValues()
	}

	return it.hasNext()
}

func (it *iterator) Current() *dynamic.Message {
	m := dynamic.NewMessage(it.schema)
	for _, field := range it.tszFields {
		m.SetFieldByNumber(field.fieldNum, math.Float64frombits(field.prevFloatBits))
	}
	return m
}

func (it *iterator) readFirstTSZValues() {
	for i := range it.tszFields {
		// Check for error here?
		fb, xor := it.readFullFloatVal()
		it.tszFields[i].prevFloatBits = fb
		it.tszFields[i].prevXOR = xor
	}

	it.consumedFirstTSZ = true
}

func (it *iterator) readNextTSZValues() {
	for i := range it.tszFields {
		// Check for error here?
		fb, xor := it.readFloatXOR(i)
		it.tszFields[i].prevFloatBits = fb
		it.tszFields[i].prevXOR = xor
	}
}

func (it *iterator) readFloatXOR(i int) (floatBits, xor uint64) {
	xor = it.readXOR(i)
	prevFloatBits := it.tszFields[i].prevFloatBits
	return prevFloatBits ^ xor, xor
}

func (it *iterator) readXOR(i int) uint64 {
	cb := it.readBits(1)
	// Share opcodeZeroValueXOR constant
	if cb == 0x0 {
		return 0
	}

	cb = (cb << 1) | it.readBits(1)
	// Share opcodeContainedValueXOR constant
	if cb == 0x2 {
		previousXOR := it.tszFields[i].prevXOR
		previousLeading, previousTrailing := encoding.LeadingAndTrailingZeros(previousXOR)
		numMeaningfulBits := 64 - previousLeading - previousTrailing
		return it.readBits(numMeaningfulBits) << uint(previousTrailing)
	}

	numLeadingZeros := int(it.readBits(6))
	numMeaningfulBits := int(it.readBits(6)) + 1
	numTrailingZeros := 64 - numLeadingZeros - numMeaningfulBits
	meaningfulBits := it.readBits(numMeaningfulBits)
	return meaningfulBits << uint(numTrailingZeros)
}

func (it *iterator) readFullFloatVal() (floatBits uint64, xor uint64) {
	floatBits = it.readBits(64)
	return floatBits, floatBits
}

func (it *iterator) readBits(numBits int) uint64 {
	if !it.hasNext() {
		return 0
	}
	res, err := it.stream.ReadBits(numBits)
	if it.err == nil && err != nil {
		it.err = err
	}
	return res
}

func (it *iterator) hasNext() bool {
	// TODO(rartoul): Do I care about closed? Maybe for cleanup
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *iterator) hasError() bool {
	return it.err != nil
}

func (i *iterator) isDone() bool {
	// TODO: Fix me
	return false
}

func (i *iterator) isClosed() bool {
	// TODO: Fix me
	return false
}
