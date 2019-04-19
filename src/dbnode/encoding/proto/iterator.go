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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	xtime "github.com/m3db/m3/src/x/time"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

const (
	// Maximum capacity of a checked.Bytes that will be retained between resets.
	maxCapacityUnmarshalBufferRetain = 1024
)

var (
	itErrPrefix                 = "proto iterator:"
	errIteratorSchemaIsRequired = fmt.Errorf("%s schema is required", itErrPrefix)
)

type iterator struct {
	opts                   encoding.Options
	err                    error
	schema                 *desc.MessageDescriptor
	stream                 encoding.IStream
	lastIterated           *dynamic.Message
	lastIteratedProtoBytes []byte
	byteFieldDictLRUSize   int
	// TODO(rartoul): Update these as we traverse the stream if we encounter
	// a mid-stream schema change: https://github.com/m3db/m3/issues/1471
	customFields []customFieldState

	tsIterator m3tsz.TimestampIterator

	// Fields that are reused between function calls to
	// avoid allocations.
	varIntBuf         [8]byte
	bitsetValues      []int
	unmarshalProtoBuf checked.Bytes

	consumedFirstMessage bool
	done                 bool
	closed               bool
}

// NewIterator creates a new iterator.
func NewIterator(
	reader io.Reader,
	schema *desc.MessageDescriptor,
	opts encoding.Options,
) encoding.ReaderIterator {
	stream := encoding.NewIStream(reader)

	var currCustomFields []customFieldState
	if schema != nil {
		currCustomFields = customFields(nil, schema)
	}
	return &iterator{
		opts:         opts,
		schema:       schema,
		stream:       stream,
		lastIterated: dynamic.NewMessage(schema),
		customFields: currCustomFields,

		tsIterator: m3tsz.NewTimestampIterator(opts, true),
	}
}

func (it *iterator) Next() bool {
	if it.schema == nil {
		it.err = errIteratorSchemaIsRequired
	}

	if !it.hasNext() {
		fmt.Println(1)
		return false
	}

	fmt.Println("a")
	moreDataControlBit, err := it.stream.ReadBit()
	if err == io.EOF {
		fmt.Println(2)
		it.done = true
		return false
	}
	if err != nil {
		fmt.Println(3)
		it.err = err
		return false
	}

	if moreDataControlBit == opCodeNoMoreDataOrTimeUnitChangeAndOrSchemaChange {
		fmt.Println("b")
		// The next bit will tell us whether we've reached the end of the stream
		// or that the time unit and/or schema has changed.
		noMoreDataControlBit, err := it.stream.ReadBit()
		if err == io.EOF {
			fmt.Println(4)
			it.done = true
			return false
		}
		if err != nil {
			fmt.Println(5)
			it.err = err
			return false
		}

		if noMoreDataControlBit == opCodeNoMoreData {
			fmt.Println(6)
			it.done = true
			return false
		}

		fmt.Println("time unit change and/or shema change!")
		// The next bit will tell us whether the time unit has changed.
		timeUnitHasChangedControlBit, err := it.stream.ReadBit()
		if err != nil {
			fmt.Println(7)
			it.err = err
			return false
		}

		fmt.Println("timeUnitHasChangedControlBit: ", timeUnitHasChangedControlBit)
		// The next bit will tell us whether the schema has changed.
		schemaHasChangedControlBit, err := it.stream.ReadBit()
		if err != nil {
			fmt.Println(8)
			it.err = err
			return false
		}

		if timeUnitHasChangedControlBit == opCodeTimeUnitChange {
			fmt.Println("time unit change!")
			fmt.Println(9)
			if err := it.tsIterator.ReadTimeUnit(it.stream); err != nil {
				fmt.Println(10)
				it.err = fmt.Errorf("%s error reading new time unit: %v", itErrPrefix, err)
				return false
			}

			if !it.consumedFirstMessage {
				fmt.Println(11)
				// Don't interpret the initial time unit as a "change" since the encoder special
				// cases the first one.
				it.tsIterator.TimeUnitChanged = false
			}
		}

		if schemaHasChangedControlBit == opCodeSchemaChange {
			if err := it.readHeader(); err != nil {
				it.err = err
				return false
			}

			// A schema change invalidates all of the existing state. This means that
			// lastIterated needs to be reset otherwise previous values for fields that
			// no longer exist would still be returned.
			it.lastIterated = dynamic.NewMessage(it.schema)
		}
	}

	fmt.Println("c")
	_, done, err := it.tsIterator.ReadTimestamp(it.stream)
	if err != nil {
		it.err = fmt.Errorf("%s error reading timestamp: %v", itErrPrefix, err)
		return false
	}
	if done {
		// This should never happen since we never encode the EndOfStream marker.
		it.err = fmt.Errorf("%s unexpected end of timestamp stream", itErrPrefix)
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

	// TODO(rartoul): Add MarshalInto method to ProtoReflect library to save
	// allocations: https://github.com/m3db/m3/issues/1471
	// Keep the annotation version of the last iterated protobuf message up to
	// date so we can return it in subsequent calls to Current(), otherwise we'd
	// have to marshal it in the Current() call where we can't handle errors.
	it.lastIteratedProtoBytes, err = it.lastIterated.Marshal()
	if err != nil {
		it.err = fmt.Errorf(
			"%s: error marshaling last iterated proto message: %v", itErrPrefix, err)
		return false
	}

	it.consumedFirstMessage = true
	return it.hasNext()
}

func (it *iterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	var (
		dp = ts.Datapoint{
			Timestamp: it.tsIterator.PrevTime,
		}
		unit = it.tsIterator.TimeUnit
	)
	return dp, unit, it.lastIteratedProtoBytes
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Reset(reader io.Reader) {
	it.stream.Reset(reader)
	it.tsIterator = m3tsz.NewTimestampIterator(it.opts, true)

	it.err = nil
	it.consumedFirstMessage = false
	it.lastIterated = dynamic.NewMessage(it.schema)
	it.lastIteratedProtoBytes = nil
	it.customFields = customFields(it.customFields, it.schema)
	it.done = false
	it.closed = false
	it.byteFieldDictLRUSize = 0
}

// SetSchema sets the encoders schema.
func (it *iterator) SetSchema(schema *desc.MessageDescriptor) {
	it.schema = schema
	it.customFields = customFields(it.customFields, it.schema)
}

func (it *iterator) Close() {
	if it.closed {
		return
	}

	it.closed = true
	it.Reset(nil)
	it.stream.Reset(nil)

	if it.unmarshalProtoBuf != nil && it.unmarshalProtoBuf.Cap() > maxCapacityUnmarshalBufferRetain {
		// Only finalize the buffer if its grown too large to prevent pooled
		// iterators from growing excessively large.
		it.unmarshalProtoBuf.DecRef()
		it.unmarshalProtoBuf.Finalize()
		it.unmarshalProtoBuf = nil
	}

	if pool := it.opts.ReaderIteratorPool(); pool != nil {
		pool.Put(it)
	}
}

func (it *iterator) readHeader() error {
	// Can ignore the version number for now because we only have one.
	_, err := it.readVarInt()
	if err != nil {
		return err
	}

	byteFieldDictLRUSize, err := it.readVarInt()
	if err != nil {
		return err
	}

	it.byteFieldDictLRUSize = int(byteFieldDictLRUSize)
	return it.readCustomFieldsSchema()
}

func (it *iterator) readCustomFieldsSchema() error {
	numCustomFields, err := it.readVarInt()
	if err != nil {
		return err
	}

	if numCustomFields > maxCustomFieldNum {
		return fmt.Errorf(
			"%s num custom fields in header is %d but maximum allowed is %d",
			itErrPrefix, numCustomFields, maxCustomFieldNum)
	}

	if it.customFields != nil {
		it.customFields = it.customFields[:0]
	} else {
		it.customFields = make([]customFieldState, 0, numCustomFields)
	}

	for i := 1; i <= int(numCustomFields); i++ {
		fieldTypeBits, err := it.stream.ReadBits(3)
		if err != nil {
			return err
		}

		fieldType := customFieldType(fieldTypeBits)
		if fieldType == notCustomEncodedField {
			continue
		}

		it.customFields = append(it.customFields, newCustomFieldState(i, fieldType))
	}

	return nil
}

func (it *iterator) readCustomValues() error {
	for i, customField := range it.customFields {
		switch {
		case isCustomFloatEncodedField(customField.fieldType):
			if err := it.readFloatValue(i); err != nil {
				return err
			}
		case customField.fieldType == bytesField:
			if err := it.readBytesValue(i, customField); err != nil {
				return err
			}
		case isCustomIntEncodedField(customField.fieldType):
			if err := it.readIntValue(i); err != nil {
				return err
			}
		default:
			return fmt.Errorf(
				"%s: unhandled custom field type: %v", itErrPrefix, customField.fieldType)
		}
	}

	return nil
}

func (it *iterator) readProtoValues() error {
	protoChangesControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf("%s err reading proto changes control bit: %v", itErrPrefix, err)
	}

	if protoChangesControlBit == opCodeNoChange {
		// No changes since previous message.
		return nil
	}

	fieldsSetToDefaultControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf("%s err reading field set to default control bit: %v", itErrPrefix, err)
	}

	if fieldsSetToDefaultControlBit == opCodeFieldsSetToDefaultProtoMarshal {
		// Some fields set to default value, need to read bitset.
		err = it.readBitset()
		if err != nil {
			return fmt.Errorf(
				"error readining changed proto field numbers bitset: %v", err)
		}
	}

	marshalLen, err := it.readVarInt()
	if err != nil {
		return fmt.Errorf("%s err reading proto length varint: %v", itErrPrefix, err)
	}

	if marshalLen > maxMarshaledProtoMessageSize {
		return fmt.Errorf(
			"%s marshaled protobuf size was %d which is larger than the maximum of %d",
			itErrPrefix, marshalLen, maxMarshaledProtoMessageSize)
	}

	it.resetUnmarshalProtoBuffer(int(marshalLen))
	unmarshalBytes := it.unmarshalProtoBuf.Bytes()
	n, err := it.stream.Read(unmarshalBytes)
	if err != nil {
		return fmt.Errorf("%s: error reading marshaled proto bytes: %v", itErrPrefix, err)
	}
	if n != int(marshalLen) {
		return fmt.Errorf(
			"%s tried to read %d marshaled proto bytes but only read %d",
			itErrPrefix, int(marshalLen), n)
	}

	m := dynamic.NewMessage(it.schema)
	err = m.Unmarshal(unmarshalBytes)
	if err != nil {
		return fmt.Errorf("error unmarshaling protobuf: %v", err)
	}

	for _, field := range m.GetKnownFields() {
		var (
			messageType = field.GetMessageType()
			fieldNumInt = int(field.GetNumber())
		)
		if messageType == nil && !field.IsRepeated() {
			continue
		}

		curVal := m.GetFieldByNumber(fieldNumInt)
		isDefaultValue, err := isDefaultValue(field, curVal)
		if err != nil {
			return fmt.Errorf(
				"%s error: %v checking if %v is default value for field %s",
				itErrPrefix, err, curVal, field.String())
		}

		if isDefaultValue {
			// The value may appear as a default value simply because it hasn't changed
			// since the last message. Ignore for now and if it truly changed to become
			// a default value it will get handled when we loop through the bitset later.
			continue
		}

		// If the unmarshaled value is not the default value for the field then
		// we know it has changed and needs to be updated.
		it.lastIterated.SetFieldByNumber(fieldNumInt, curVal)
	}

	if fieldsSetToDefaultControlBit == 1 {
		for _, fieldNum := range it.bitsetValues {
			if err := it.lastIterated.TryClearFieldByNumber(fieldNum); err != nil {
				return fmt.Errorf(
					"%s: error clearing field number: %d, err: %v",
					itErrPrefix, fieldNum, err)
			}
		}
	}

	return nil
}

func (it *iterator) readFloatValue(i int) error {
	if err := it.customFields[i].floatEncAndIter.ReadFloat(it.stream); err != nil {
		return err
	}

	return it.updateLastIteratedWithCustomValues(i)
}

func (it *iterator) readBytesValue(i int, customField customFieldState) error {
	bytesChangedControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"%s: error trying to read bytes changed control bit: %v",
			itErrPrefix, err)
	}

	if bytesChangedControlBit == opCodeNoChange {
		// No changes to the bytes value.
		return nil
	}

	// Bytes have changed since the previous value.
	valueInDictControlBit, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"%s error trying to read bytes changed control bit: %v",
			itErrPrefix, err)
	}

	if valueInDictControlBit == opCodeInterpretSubsequentBitsAsLRUIndex {
		dictIdxBits, err := it.stream.ReadBits(
			numBitsRequiredForNumUpToN(it.byteFieldDictLRUSize))
		if err != nil {
			return fmt.Errorf(
				"%s error trying to read bytes dict idx: %v",
				itErrPrefix, err)
		}

		dictIdx := int(dictIdxBits)
		if dictIdx >= len(customField.iteratorBytesFieldDict) || dictIdx < 0 {
			return fmt.Errorf(
				"%s read bytes field dictionary index: %d, but dictionary is size: %d",
				itErrPrefix, dictIdx, len(customField.iteratorBytesFieldDict))
		}

		bytesVal := customField.iteratorBytesFieldDict[dictIdx]
		if it.schema.FindFieldByNumber(int32(customField.fieldNum)).GetType() == dpb.FieldDescriptorProto_TYPE_STRING {
			it.lastIterated.SetFieldByNumber(customField.fieldNum, string(bytesVal))
		} else {
			it.lastIterated.SetFieldByNumber(customField.fieldNum, bytesVal)
		}

		it.moveToEndOfBytesDict(i, dictIdx)
		return nil
	}

	// New value that was not in the dict already.
	bytesLen, err := it.readVarInt()
	if err != nil {
		return fmt.Errorf(
			"%s error trying to read bytes length: %v", itErrPrefix, err)
	}

	if err := it.skipToNextByte(); err != nil {
		return fmt.Errorf(
			"%s error trying to skip bytes value bit padding: %v",
			itErrPrefix, err)
	}

	buf := make([]byte, 0, bytesLen)
	for j := 0; j < int(bytesLen); j++ {
		b, err := it.stream.ReadByte()
		if err != nil {
			return fmt.Errorf(
				"%s error trying to read byte in readBytes: %v",
				itErrPrefix, err)
		}
		buf = append(buf, b)
	}

	// TODO(rartoul): Could make this more efficient with unsafe string conversion or by pre-processing
	// schemas to only have bytes since its all the same over the wire.
	// https://github.com/m3db/m3/issues/1471
	fmt.Println("hmm", it.schema.FindFieldByNumber(int32(customField.fieldNum)))
	fmt.Println("hmm", customField.fieldNum)
	schemaField := it.schema.FindFieldByNumber(int32(customField.fieldNum))
	if schemaField != nil {
		// schemaField can be nil in the case where we're decoding a stream that was encoded with a schema
		// newer than the one we currently have.
		schemaFieldType := schemaField.GetType()
		if schemaFieldType == dpb.FieldDescriptorProto_TYPE_STRING {
			err = it.lastIterated.TrySetFieldByNumber(customField.fieldNum, string(buf))
		} else {
			err = it.lastIterated.TrySetFieldByNumber(customField.fieldNum, buf)
		}
		if err != nil {
			return fmt.Errorf(
				"%s error trying to set field number: %d, err: %v",
				itErrPrefix, customField.fieldNum, err)
		}
	}

	it.addToBytesDict(i, buf)
	return nil
}

func (it *iterator) readIntValue(i int) error {
	if err := it.customFields[i].intEncAndIter.readIntValue(it.stream); err != nil {
		return err
	}

	return it.updateLastIteratedWithCustomValues(i)
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

	if field := it.schema.FindFieldByNumber(int32(fieldNum)); field == nil {
		// This can happen when the field being encoded does not exist in
		// the current schema (or is reserved), but the message was encoded
		// with a schema in which the field number did exist.
		return nil
	}

	switch {
	case isCustomFloatEncodedField(fieldType):
		var (
			val = math.Float64frombits(it.customFields[i].floatEncAndIter.PrevFloatBits)
			err error
		)
		if fieldType == float64Field {
			err = it.lastIterated.TrySetFieldByNumber(fieldNum, val)
		} else {
			err = it.lastIterated.TrySetFieldByNumber(fieldNum, float32(val))
		}
		return err

	case isCustomIntEncodedField(fieldType):
		switch fieldType {
		case signedInt64Field:
			val := int64(it.customFields[i].intEncAndIter.prevIntBits)
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)

		case unsignedInt64Field:
			val := it.customFields[i].intEncAndIter.prevIntBits
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)

		case signedInt32Field:
			val := int32(it.customFields[i].intEncAndIter.prevIntBits)
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)

		case unsignedInt32Field:
			val := uint32(it.customFields[i].intEncAndIter.prevIntBits)
			return it.lastIterated.TrySetFieldByNumber(fieldNum, val)

		default:
			return fmt.Errorf(
				"%s expected custom int encoded field but field type was: %v",
				itErrPrefix, fieldType)
		}
	default:
		return fmt.Errorf(
			"%s unhandled fieldType: %v", itErrPrefix, fieldType)
	}
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
			return fmt.Errorf("%s error reading bitset: %v", itErrPrefix, err)
		}

		if bit == opCodeBitsetValueIsSet {
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
			return 0, fmt.Errorf("%s error reading var int: %v", itErrPrefix, err)
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

// skipToNextByte will skip over any remaining bits in the current byte
// to reach the next byte. This is used in situations where the stream
// has padding bits to keep portions of data aligned at the byte boundary.
func (it *iterator) skipToNextByte() error {
	remainingBitsInByte := it.stream.RemainingBitsInCurrentByte()
	for remainingBitsInByte > 0 {
		_, err := it.stream.ReadBit()
		if err != nil {
			return err
		}
		remainingBitsInByte--
	}

	return nil
}

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
}

func (it *iterator) addToBytesDict(fieldIdx int, b []byte) {
	existing := it.customFields[fieldIdx].iteratorBytesFieldDict
	if len(existing) < it.byteFieldDictLRUSize {
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
}

func (it *iterator) readBits(numBits int) (uint64, error) {
	res, err := it.stream.ReadBits(numBits)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func (it *iterator) resetUnmarshalProtoBuffer(n int) {
	if it.unmarshalProtoBuf != nil && it.unmarshalProtoBuf.Cap() >= n {
		// If the existing one is big enough, just resize it.
		it.unmarshalProtoBuf.Resize(n)
		return
	}

	if it.unmarshalProtoBuf != nil {
		// If one exists, but its too small, return it to the pool.
		it.unmarshalProtoBuf.DecRef()
		it.unmarshalProtoBuf.Finalize()
	}

	// If none exists (or one existed but it was too small) get a new one
	// and IncRef(). DecRef() will never be called unless this one is
	// replaced by a new one later.
	it.unmarshalProtoBuf = it.opts.BytesPool().Get(n)
	it.unmarshalProtoBuf.IncRef()
	it.unmarshalProtoBuf.Resize(n)
}

func (it *iterator) hasNext() bool {
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *iterator) hasError() bool {
	return it.err != nil
}

func (it *iterator) isDone() bool {
	return it.done
}

func (it *iterator) isClosed() bool {
	return it.closed
}
