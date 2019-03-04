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
	"encoding/binary"
	"fmt"
	"io"
	"math"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
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

	it.readTSZValues()
	it.readProtoValues()

	return it.hasNext()
}

func (it *iterator) readTSZValues() {
	if !it.consumedFirstTSZ {
		it.readFirstTSZValues()
	} else {
		it.readNextTSZValues()
	}
}

func (it *iterator) readProtoValues() {
	bit, err := it.stream.ReadBit()
	if err != nil {
		it.err = err
		return
	}

	if bit == 0 {
		// No changes since previous message.
		return
	}

	// TODO: Check error after this function call
	// TODO: if a field exists in the changedbitset,
	// but we don't have an explicit value for it in the unmarshaled
	// message that means the caller set it to a default value.
	// So we need to handle that here
	changedFieldNums := it.readBitset()

	// TODO: Check error after this?
	marshalLen := it.readVarInt()
	buf := make([]byte, 0, marshalLen)
	for i := uint64(0); i < marshalLen; i++ {
		b, err := it.stream.ReadByte()
		if err != nil {
			it.err = fmt.Errorf("error reading marshaled proto bytes: %v", err)
			return
		}
		buf = append(buf, b)
	}

	if it.lastIterated == nil {
		it.lastIterated = dynamic.NewMessage(it.schema)
	}

	currMessage := dynamic.NewMessage(it.schema)
	err = currMessage.Unmarshal(buf)
	if err != nil {
		it.err = fmt.Errorf("error unmarshaling protobuf: %v", err)
		return
	}

	it.lastIterated.MergeFrom(currMessage)

	// Loop through all changed fields
	// if they are "default value" in the new unmarshaled message
	// set them to default value in the old message
	for _, fieldNum := range changedFieldNums {
		var (
			fieldDesc         = it.schema.FindFieldByNumber(int32(fieldNum))
			fieldDefaultValue = fieldDesc.GetDefaultValue()
			existingVal       = currMessage.GetFieldByNumber(fieldNum)
		)
		if fieldsEqual(existingVal, fieldDefaultValue) {
			it.lastIterated.ClearFieldByNumber(fieldNum)
		}
	}
}

func (it *iterator) readBitset() []int {
	vals := []int{}
	bitsetLengthBits := it.readVarInt()
	for i := uint64(0); i < bitsetLengthBits; i++ {
		bit, err := it.stream.ReadBit()
		// TODO: This function should just return an error
		if err != nil {
			it.err = fmt.Errorf("error reading bitset: %v", err)
			return nil
		}

		if bit == 1 {
			// Add 1 because protobuf fields are 1-indexed not 0-indexed.
			vals = append(vals, int(i)+1)
		}
	}

	return vals
}

func (it *iterator) readVarInt() uint64 {
	// TODO: Reuse
	buf := make([]byte, 0, 0)
	for {
		b, err := it.stream.ReadByte()
		if err != nil {
			// TODO: SHOULD THIS function just return an error
			it.err = fmt.Errorf("error reading var int: %v", err)
			return 0
		}
		buf = append(buf, b)
		if b>>7 == 0 {
			break
		}
	}

	varInt, _ := binary.Uvarint(buf)
	return varInt
}

func (it *iterator) Current() *dynamic.Message {
	return it.lastIterated
}

func (it *iterator) readFirstTSZValues() {
	for i := range it.tszFields {
		// Check for error here?
		fb, xor := it.readFullFloatVal()
		it.tszFields[i].prevFloatBits = fb
		it.tszFields[i].prevXOR = xor
		it.updateLastIteratedWithTSZValues(i)
	}

	it.consumedFirstTSZ = true
}

func (it *iterator) readNextTSZValues() {
	for i := range it.tszFields {
		// Check for error here?
		fb, xor := it.readFloatXOR(i)
		it.tszFields[i].prevFloatBits = fb
		it.tszFields[i].prevXOR = xor
		it.updateLastIteratedWithTSZValues(i)
	}
}

// updateLastIteratedWithTSZValues updates lastIterated with the current
// value of the TSZ field in it.tszFields at index i. This ensures that
// when we return it.lastIterated in the call to Current() that all the
// most recent values are present.
func (it *iterator) updateLastIteratedWithTSZValues(i int) {
	if it.lastIterated == nil {
		it.lastIterated = dynamic.NewMessage(it.schema)
	}

	var (
		fieldNum = it.tszFields[i].fieldNum
		val      = math.Float64frombits(it.tszFields[i].prevFloatBits)
		err      error
	)
	if it.schema.FindFieldByNumber(int32(fieldNum)).GetType() == dpb.FieldDescriptorProto_TYPE_DOUBLE {
		err = it.lastIterated.TrySetFieldByNumber(fieldNum, val)
	} else {
		err = it.lastIterated.TrySetFieldByNumber(fieldNum, float32(val))
	}
	if err != nil {
		// TODO: Fix me
		it.err = err
	}
}

func (it *iterator) readFloatXOR(i int) (floatBits, xor uint64) {
	xor = it.readXOR(i)
	prevFloatBits := it.tszFields[i].prevFloatBits
	return prevFloatBits ^ xor, xor
}

func (it *iterator) readXOR(i int) uint64 {
	cb := it.readBits(1)
	if cb == m3tsz.OpcodeZeroValueXOR {
		return 0
	}

	cb = (cb << 1) | it.readBits(1)
	if cb == m3tsz.OpcodeContainedValueXOR {
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
