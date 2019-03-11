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
	"errors"
	"fmt"
	"io"
	"math"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
)

var (
	errIteratorReaderIsRequired = errors.New("proto iterator: reader is required")
	errIteratorSchemaIsRequired = errors.New("proto iterator: schema is required")
)

type iterator struct {
	err                  error
	schema               *desc.MessageDescriptor
	stream               encoding.IStream
	consumedFirstMessage bool
	lastIterated         *dynamic.Message
	customFields         []customFieldState

	// Fields that are reused between function calls to
	// avoid allocations.
	varIntBuf    [8]byte
	bitsetValues []int

	done bool
}

// NewIterator creates a new iterator.
func NewIterator(
	reader io.Reader,
	schema *desc.MessageDescriptor,
) (*iterator, error) {
	if reader == nil {
		return nil, errIteratorReaderIsRequired
	}
	if schema == nil {
		return nil, errIteratorSchemaIsRequired
	}

	iter := &iterator{
		schema:       schema,
		stream:       encoding.NewIStream(reader),
		lastIterated: dynamic.NewMessage(schema),
		// TODO: These need to be possibly updated as we traverse a stream
		customFields: customFields(nil, schema),
	}

	return iter, nil
}

func (it *iterator) Next() bool {
	if !it.hasNext() {
		return false
	}

	moreDataControlBit, err := it.stream.ReadBit()
	if err == io.EOF || (err == nil && moreDataControlBit == 0) {
		it.done = true
		return false
	}

	if err != nil {
		it.err = err
		return false
	}

	if err := it.readCustomValues(); err != nil {
		it.err = err
		return false
	}

	if err := it.readProtoValues(); err != nil {
		it.err = err
		return false
	}

	it.consumedFirstMessage = true
	return it.hasNext()
}

func (it *iterator) Current() *dynamic.Message {
	return it.lastIterated
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) readCustomValues() error {
	var err error

	if !it.consumedFirstMessage {
		err = it.readFirstCustomValues()
	} else {
		err = it.readNextCustomValues()
	}

	return err
}

func (it *iterator) readProtoValues() error {
	protoChangesControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf("proto iterator: err reading proto changes control bit: %v", err)
	}

	if protoChangesControlBit == 0 {
		// No changes since previous message.
		return nil
	}

	fieldsSetToDefaultControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf("proto iterator: err reading field set to default control bit: %v", err)
	}

	if fieldsSetToDefaultControlBit == 1 {
		// Some fields set to default value, need to read bitset.
		err = it.readBitset()
		if err != nil {
			return fmt.Errorf(
				"error readining changed proto field numbers bitset: %v", err)
		}
	}

	marshalLen, err := it.readVarInt()
	if err != nil {
		return fmt.Errorf("proto iterator: err reading proto length varint: %v", err)
		return err
	}

	// TODO(rartoul): Probably want to use a bytes pool for this.
	// TODO: Use it.stream.Read()
	buf := make([]byte, 0, marshalLen)
	for i := uint64(0); i < marshalLen; i++ {
		b, err := it.stream.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading marshaled proto bytes: %v", err)
		}
		buf = append(buf, b)
	}

	if it.lastIterated == nil {
		it.lastIterated = dynamic.NewMessage(it.schema)
	}

	err = it.lastIterated.UnmarshalMerge(buf)
	if err != nil {
		return fmt.Errorf("error unmarshaling protobuf: %v", err)
	}

	if fieldsSetToDefaultControlBit == 1 {
		for _, fieldNum := range it.bitsetValues {
			// TODO: Try
			it.lastIterated.ClearFieldByNumber(fieldNum)
		}
	}

	return nil
}

func (it *iterator) readFirstCustomValues() error {
	for i, customField := range it.customFields {
		if customField.fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE {
			if err := it.readFirstTSZValue(i, customField); err != nil {
				return err
			}
		}

		if customField.fieldType == dpb.FieldDescriptorProto_TYPE_BYTES {
			if err := it.readBytesValue(i, customField); err != nil {
				return err
			}
		}

		if isCustomIntEncodedField(customField.fieldType) {
			if err := it.readIntValue(i, customField, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (it *iterator) readFirstTSZValue(i int, customField customFieldState) error {
	fb, xor, err := it.readFullFloatVal()
	if err != nil {
		return err
	}

	it.customFields[i].prevFloatBits = fb
	it.customFields[i].prevXOR = xor
	if err := it.updateLastIteratedWithCustomValues(i); err != nil {
		return err
	}

	return nil
}

func (it *iterator) readNextCustomValues() error {
	for i, customField := range it.customFields {
		if customField.fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE {
			if err := it.readNextTSZValue(i, customField); err != nil {
				return err
			}
		}

		if customField.fieldType == dpb.FieldDescriptorProto_TYPE_BYTES {
			if err := it.readBytesValue(i, customField); err != nil {
				return err
			}
		}

		if isCustomIntEncodedField(customField.fieldType) {
			if err := it.readIntValue(i, customField, false); err != nil {
				return err
			}
		}
	}

	return nil
}

func (it *iterator) readNextTSZValue(i int, customField customFieldState) error {
	fb, xor, err := it.readFloatXOR(i)
	if err != nil {
		return err
	}

	it.customFields[i].prevFloatBits = fb
	it.customFields[i].prevXOR = xor
	if err := it.updateLastIteratedWithCustomValues(i); err != nil {
		return err
	}

	return nil
}

func (it *iterator) readIntValue(i int, customField customFieldState, first bool) error {
	if !first {
		changeExistsControlBit, err := it.stream.ReadBit()
		if err != nil {
			return fmt.Errorf("proto decoder: error trying to read int change exists control bit: %v", err)
		}

		if changeExistsControlBit == 0 {
			// No change.
			return nil
		}
	}

	if err := it.readIntSig(i); err != nil {
		return fmt.Errorf("proto decoder: error trying to read number of significant digits: %v", err)
	}

	if err := it.readIntValDiff(i); err != nil {
		return fmt.Errorf("proto decoder: error trying to read int diff: %v", err)
	}

	if err := it.updateLastIteratedWithCustomValues(i); err != nil {
		return fmt.Errorf("proto decoder: error updating last iterated with int value: %v", err)
	}
	return nil
}

func (it *iterator) readBytesValue(i int, customField customFieldState) error {
	bytesChangedControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"proto decoder: error trying to read bytes changed control bit: %v", err)
	}

	if bytesChangedControlBit == 0 {
		// No changes to the bytes value.
		return nil
	}

	// Bytes have changed since the previous value.
	valueInDictControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"proto decoder: error trying to read bytes changed control bit: %v", err)
	}
	if valueInDictControlBit == 0 {
		// TODO: Make number of bits auto-detect from size
		dictIdx, err := it.stream.ReadBits(2)
		if err != nil {
			return fmt.Errorf(
				"proto decoder: error trying to read bytes dict idx: %v", err)
		}

		bytesVal := customField.iteratorBytesFieldDict[int(dictIdx)]
		if it.schema.FindFieldByNumber(int32(customField.fieldNum)).GetType() == dpb.FieldDescriptorProto_TYPE_STRING {
			it.lastIterated.SetFieldByNumber(customField.fieldNum, string(bytesVal))
		} else {
			it.lastIterated.SetFieldByNumber(customField.fieldNum, bytesVal)
		}
		// TODO: Handle out of bounds error or anything here?
		it.moveToEndOfBytesDict(i, int(dictIdx))
		return nil
	}

	// New value that was not in the dict already.
	bytesLen, err := it.readVarInt()
	if err != nil {
		return fmt.Errorf(
			"proto decoder: error trying to read bytes length: %v", err)
	}

	// TODO: Corrupt data could cause a panic here by doing a massive alloc
	buf := make([]byte, 0, bytesLen)
	for j := 0; j < int(bytesLen); j++ {
		b, err := it.stream.ReadByte()
		if err != nil {
			return fmt.Errorf("proto decoder: error trying to read byte in readBytes: %v", err)
		}
		buf = append(buf, b)
	}
	// TODO: Can re-use this instead of allocating the temporary buffer.
	it.customFields[i].prevBytes = buf
	// TODO: switch to try
	// TODO: Make this less gross by pre-processing schemas to only have bytes
	if it.schema.FindFieldByNumber(int32(customField.fieldNum)).GetType() == dpb.FieldDescriptorProto_TYPE_STRING {
		it.lastIterated.SetFieldByNumber(customField.fieldNum, string(buf))
	} else {
		it.lastIterated.SetFieldByNumber(customField.fieldNum, buf)
	}
	it.addToBytesDict(i, buf)

	return nil
}

// updateLastIteratedWithCustomValues updates lastIterated with the current
// value of the custom field in it.customFields at index i. This ensures that
// when we return it.lastIterated in the call to Current() that all the
// most recent values are present.
func (it *iterator) updateLastIteratedWithCustomValues(i int) error {
	if it.lastIterated == nil {
		it.lastIterated = dynamic.NewMessage(it.schema)
	}

	var (
		fieldNum  = it.customFields[i].fieldNum
		fieldType = it.customFields[i].fieldType
	)
	// TODO: Not sure I need to handle both cases here. Might be ok to just check for double.
	if fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE ||
		fieldType == dpb.FieldDescriptorProto_TYPE_FLOAT {
		var (
			val = math.Float64frombits(it.customFields[i].prevFloatBits)
			err error
		)
		// TODO: Should I be looking at schema here or field types?
		if it.schema.FindFieldByNumber(int32(fieldNum)).GetType() == dpb.FieldDescriptorProto_TYPE_DOUBLE {
			err = it.lastIterated.TrySetFieldByNumber(fieldNum, val)
		} else {
			err = it.lastIterated.TrySetFieldByNumber(fieldNum, float32(val))
		}
		return err
	} else if isCustomIntEncodedField(fieldType) {
		// TODO: same comment here
		if fieldType == dpb.FieldDescriptorProto_TYPE_INT64 ||
			fieldType == dpb.FieldDescriptorProto_TYPE_SFIXED64 ||
			fieldType == dpb.FieldDescriptorProto_TYPE_SINT64 {
			val := int64(it.customFields[i].prevFloatBits)
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)
		}

		if fieldType == dpb.FieldDescriptorProto_TYPE_UINT64 {
			val := it.customFields[i].prevFloatBits
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)
		}

		val := int32(it.customFields[i].prevFloatBits)
		return it.lastIterated.TrySetFieldByNumber(fieldNum, val)
	} else {
		return fmt.Errorf("proto decoder: unhandled fieldType: %v", fieldType)
	}
}

func (it *iterator) readFloatXOR(i int) (floatBits, xor uint64, err error) {
	xor, err = it.readXOR(i)
	if err != nil {
		return 0, 0, err
	}
	prevFloatBits := it.customFields[i].prevFloatBits
	return prevFloatBits ^ xor, xor, nil
}

func (it *iterator) readXOR(i int) (uint64, error) {
	cb, err := it.readBits(1)
	if err != nil {
		return 0, err
	}
	if cb == m3tsz.OpcodeZeroValueXOR {
		return 0, nil
	}

	cb2, err := it.readBits(1)
	if err != nil {
		return 0, err
	}

	cb = (cb << 1) | cb2
	if cb == m3tsz.OpcodeContainedValueXOR {
		var (
			previousXOR                       = it.customFields[i].prevXOR
			previousLeading, previousTrailing = encoding.LeadingAndTrailingZeros(previousXOR)
			numMeaningfulBits                 = 64 - previousLeading - previousTrailing
		)
		meaningfulBits, err := it.readBits(numMeaningfulBits)
		if err != nil {
			return 0, err
		}

		return meaningfulBits << uint(previousTrailing), nil
	}

	numLeadingZerosBits, err := it.readBits(6)
	if err != nil {
		return 0, err
	}
	numMeaningfulBitsBits, err := it.readBits(6)
	if err != nil {
		return 0, err
	}

	var (
		numLeadingZeros   = int(numLeadingZerosBits)
		numMeaningfulBits = int(numMeaningfulBitsBits) + 1
		numTrailingZeros  = 64 - numLeadingZeros - numMeaningfulBits
	)
	meaningfulBits, err := it.readBits(numMeaningfulBits)
	if err != nil {
		return 0, err
	}

	return meaningfulBits << uint(numTrailingZeros), nil
}

func (it *iterator) readFullFloatVal() (floatBits uint64, xor uint64, err error) {
	floatBits, err = it.readBits(64)
	if err != nil {
		return 0, 0, err
	}

	return floatBits, floatBits, nil
}

// readBitset does the inverse of encodeBitset on the encoder struct.
func (it *iterator) readBitset() error {
	it.bitsetValues = it.bitsetValues[:0]
	bitsetLengthBits, err := it.readVarInt()
	if err != nil {
		return err
	}

	for i := uint64(0); i < bitsetLengthBits; i++ {
		bit, err := it.stream.ReadBit()
		if err != nil {
			return fmt.Errorf("error reading bitset: %v", err)
		}

		if bit == 1 {
			// Add 1 because protobuf fields are 1-indexed not 0-indexed.
			it.bitsetValues = append(it.bitsetValues, int(i)+1)
		}
	}

	return nil
}

func (it *iterator) readVarInt() (uint64, error) {
	var (
		// Convert array to slice and reset size to zero so
		// we can reuse the buffer.
		buf      = it.varIntBuf[:0]
		numBytes = 0
	)
	for {
		b, err := it.stream.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("error reading var int: %v", err)
		}

		buf = append(buf, b)
		numBytes++

		if b>>7 == 0 {
			break
		}
	}

	buf = buf[:numBytes]
	varInt, _ := binary.Uvarint(buf)
	return varInt, nil
}

func (it *iterator) readIntSig(i int) error {
	updateControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"proto iterator: error reading int significant digits update control bit: %v", err)
	}
	if updateControlBit == 0 {
		// No change.
		return nil
	}

	nonZeroSignificantDigitsControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"proto iterator: error reading zero significant digits control bit: %v", err)
	}
	if nonZeroSignificantDigitsControlBit == 0 {
		it.customFields[i].prevSig = 0
	} else {
		numSigBits, err := it.readBits(6)
		if err != nil {
			return fmt.Errorf(
				"proto iterator: error reading number of significant digits: %v", err)
		}

		it.customFields[i].prevSig = uint8(numSigBits) + 1
	}

	return nil
}

func (it *iterator) readIntValDiff(i int) error {
	negativeControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"proto iterator: error reading negative control bit: %v", err)
	}

	numSig := int(it.customFields[i].prevSig)

	diffSigBits, err := it.readBits(numSig)
	if err != nil {
		return fmt.Errorf(
			"proto iterator: error reading significant digits: %v", err)
	}

	if it.customFields[i].fieldType == dpb.FieldDescriptorProto_TYPE_UINT64 {
		diff := uint64(diffSigBits)
		shouldSubtract := false
		if negativeControlBit == 1 {
			shouldSubtract = true
		}

		prev := uint64(it.customFields[i].prevFloatBits)
		if shouldSubtract {
			it.customFields[i].prevFloatBits = prev - diff
		} else {
			it.customFields[i].prevFloatBits = prev + diff
		}
	} else {
		diff := int64(diffSigBits)
		sign := int64(1)
		if negativeControlBit == 1 {
			sign = -1.0
		}

		prev := int64(it.customFields[i].prevFloatBits)
		it.customFields[i].prevFloatBits = uint64(prev + (sign * diff))
	}

	return nil
}

// TODO: Share logic with encoder if possible
func (it *iterator) moveToEndOfBytesDict(fieldIdx, i int) {
	existing := it.customFields[fieldIdx].iteratorBytesFieldDict
	for j := i; j < len(existing); j++ {
		nextIdx := j + 1
		if nextIdx >= len(existing) {
			break
		}

		currVal := existing[j]
		nextVal := existing[nextIdx]
		existing[j] = nextVal
		existing[nextIdx] = currVal
	}
	// TODO: Not necessary
	it.customFields[fieldIdx].iteratorBytesFieldDict = existing
}

// TODO: Share logic with encoder if possible
func (it *iterator) addToBytesDict(fieldIdx int, b []byte) {
	existing := it.customFields[fieldIdx].iteratorBytesFieldDict
	if len(existing) < byteFieldDictSize {
		it.customFields[fieldIdx].iteratorBytesFieldDict = append(existing, b)
		return
	}

	// Shift everything down 1 and replace the last value to evict the
	// least recently used entry and add the newest one.
	//     [1,2,3]
	// becomes
	//     [2,3,3]
	// after shift, and then becomes
	//     [2,3,4]
	// after replacing the last value.
	for i := range existing {
		nextIdx := i + 1
		if nextIdx >= len(existing) {
			break
		}

		existing[i] = existing[nextIdx]
	}

	existing[len(existing)-1] = b
	// TODO: Not necessary
	it.customFields[fieldIdx].iteratorBytesFieldDict = existing
}

func (it *iterator) readBits(numBits int) (uint64, error) {
	res, err := it.stream.ReadBits(numBits)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (it *iterator) hasNext() bool {
	// TODO(rartoul): Do I care about closed? Maybe for cleanup
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *iterator) hasError() bool {
	return it.err != nil
}

func (i *iterator) isDone() bool {
	return i.done
}

func (i *iterator) isClosed() bool {
	// TODO: Fix me
	return false
}
