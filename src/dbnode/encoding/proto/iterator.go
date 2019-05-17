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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

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
	nsID                   ident.ID
	opts                   encoding.Options
	err                    error
	schema                 *desc.MessageDescriptor
	schemaDesc             namespace.SchemaDescr
	stream                 encoding.IStream
	marshaler              customFieldMarshaler
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
	descr namespace.SchemaDescr,
	opts encoding.Options,
) encoding.ReaderIterator {
	stream := encoding.NewIStream(reader)

	i := &iterator{
		opts:       opts,
		stream:     stream,
		marshaler:  newCustomMarshaler(),
		tsIterator: m3tsz.NewTimestampIterator(opts, true),
	}
	i.resetSchema(descr)
	return i
}

func (it *iterator) Next() bool {
	if it.schema == nil {
		// It is a programmatic error that schema is not set at all prior to iterating, panic to fix it asap.
		it.err = instrument.InvariantErrorf(errIteratorSchemaIsRequired.Error())
		return false
	}

	if !it.hasNext() {
		return false
	}

	if it.lastIteratedProtoBytes != nil {
		it.lastIteratedProtoBytes = it.lastIteratedProtoBytes[:0]
	}
	it.marshaler.reset()

	if !it.consumedFirstMessage {
		if err := it.readStreamHeader(); err != nil {
			it.err = fmt.Errorf(
				"%s error reading stream header: %v",
				itErrPrefix, err)
			return false
		}
	}

	moreDataControlBit, err := it.stream.ReadBit()
	if err == io.EOF {
		it.done = true
		return false
	}
	if err != nil {
		it.err = fmt.Errorf(
			"%s error reading more data control bit: %v",
			itErrPrefix, err)
		return false
	}

	if moreDataControlBit == opCodeNoMoreDataOrTimeUnitChangeAndOrSchemaChange {
		// The next bit will tell us whether we've reached the end of the stream
		// or that the time unit and/or schema has changed.
		noMoreDataControlBit, err := it.stream.ReadBit()
		if err == io.EOF {
			it.done = true
			return false
		}
		if err != nil {
			it.err = fmt.Errorf(
				"%s error reading no more data control bit: %v",
				itErrPrefix, err)
			return false
		}

		if noMoreDataControlBit == opCodeNoMoreData {
			it.done = true
			return false
		}

		// The next bit will tell us whether the time unit has changed.
		timeUnitHasChangedControlBit, err := it.stream.ReadBit()
		if err != nil {
			it.err = fmt.Errorf(
				"%s error reading time unit change has changed control bit: %v",
				itErrPrefix, err)
			return false
		}

		// The next bit will tell us whether the schema has changed.
		schemaHasChangedControlBit, err := it.stream.ReadBit()
		if err != nil {
			it.err = fmt.Errorf(
				"%s error reading schema has changed control bit: %v",
				itErrPrefix, err)
			return false
		}

		if timeUnitHasChangedControlBit == opCodeTimeUnitChange {
			if err := it.tsIterator.ReadTimeUnit(it.stream); err != nil {
				it.err = fmt.Errorf("%s error reading new time unit: %v", itErrPrefix, err)
				return false
			}

			if !it.consumedFirstMessage {
				// Don't interpret the initial time unit as a "change" since the encoder special
				// cases the first one.
				it.tsIterator.TimeUnitChanged = false
			}
		}

		if schemaHasChangedControlBit == opCodeSchemaChange {
			if err := it.readCustomFieldsSchema(); err != nil {
				it.err = fmt.Errorf("%s error reading custom fields schema: %v", itErrPrefix, err)
				return false
			}

			// A schema change invalidates all of the existing state. This means that
			// lastIterated needs to be reset otherwise previous values for fields that
			// no longer exist would still be returned.
			it.lastIterated = dynamic.NewMessage(it.schema)
		}
	}

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

	// Keep the annotation version of the last iterated protobuf message up to
	// date so we can return it in subsequent calls to Current(), otherwise we'd
	// have to marshal it in the Current() call where we can't handle errors.
	it.lastIteratedProtoBytes, err = it.lastIterated.MarshalAppend(
		it.lastIteratedProtoBytes[:0])
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
	return dp, unit, append(it.lastIteratedProtoBytes, it.marshaler.bytes()...)
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Reset(reader io.Reader, descr namespace.SchemaDescr) {
	it.resetSchema(descr)
	it.stream.Reset(reader)
	if it.lastIterated != nil {
		it.lastIterated.Reset()
	}
	it.tsIterator = m3tsz.NewTimestampIterator(it.opts, true)

	it.err = nil
	it.consumedFirstMessage = false
	it.lastIteratedProtoBytes = nil
	it.done = false
	it.closed = false
	it.byteFieldDictLRUSize = 0
}

// setSchema sets the schema for the iterator.
func (it *iterator) resetSchema(schemaDesc namespace.SchemaDescr) {
	if schemaDesc == nil {
		it.schemaDesc = nil
		it.schema = nil
		it.lastIterated = nil
		it.customFields = nil
		return
	}

	it.schemaDesc = schemaDesc
	it.schema = schemaDesc.Get().MessageDescriptor
	it.lastIterated = dynamic.NewMessage(it.schema)
	it.customFields, _ = customAndProtoFields(it.customFields, nil, it.schema)
}

func (it *iterator) Close() {
	if it.closed {
		return
	}

	it.closed = true
	it.Reset(nil, nil)
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

func (it *iterator) readStreamHeader() error {
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
	return nil
}

func (it *iterator) readCustomFieldsSchema() error {
	numCustomFields, err := it.readVarInt()
	if err != nil {
		return err
	}

	if numCustomFields > maxCustomFieldNum {
		return fmt.Errorf(
			"num custom fields in header is %d but maximum allowed is %d",
			numCustomFields, maxCustomFieldNum)
	}

	if it.customFields != nil {
		it.customFields = it.customFields[:0]
	} else {
		it.customFields = make([]customFieldState, 0, numCustomFields)
	}

	for i := 1; i <= int(numCustomFields); i++ {
		fieldTypeBits, err := it.stream.ReadBits(numBitsToEncodeCustomType)
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
		case isCustomIntEncodedField(customField.fieldType):
			if err := it.readIntValue(i); err != nil {
				return err
			}
		case customField.fieldType == bytesField:
			if err := it.readBytesValue(i, customField); err != nil {
				return err
			}
		case customField.fieldType == boolField:
			if err := it.readBoolValue(i); err != nil {
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

	// TODO: Iterate through proto fields directly.
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

	updateArg := updateLastIterArg{i: i}
	return it.updateLastIteratedWithCustomValues(updateArg)
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
		updateArg := updateLastIterArg{i: i, bytesFieldBuf: it.lastValueBytesDict(i)}
		return it.updateLastIteratedWithCustomValues(updateArg)
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
		it.moveToEndOfBytesDict(i, dictIdx)

		updateArg := updateLastIterArg{i: i, bytesFieldBuf: bytesVal}
		return it.updateLastIteratedWithCustomValues(updateArg)
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

	it.addToBytesDict(i, buf)

	updateArg := updateLastIterArg{i: i, bytesFieldBuf: buf}
	return it.updateLastIteratedWithCustomValues(updateArg)
}

func (it *iterator) readIntValue(i int) error {
	if err := it.customFields[i].intEncAndIter.readIntValue(it.stream); err != nil {
		return err
	}

	updateArg := updateLastIterArg{i: i}
	return it.updateLastIteratedWithCustomValues(updateArg)
}

func (it *iterator) readBoolValue(i int) error {
	boolOpCode, err := it.stream.ReadBit()
	if err != nil {
		return fmt.Errorf(
			"%s: error trying to read bool value: %v",
			itErrPrefix, err)
	}

	boolVal := boolOpCode == opCodeBoolTrue
	updateArg := updateLastIterArg{i: i, boolVal: boolVal}
	return it.updateLastIteratedWithCustomValues(updateArg)
}

type updateLastIterArg struct {
	i             int
	bytesFieldBuf []byte
	boolVal       bool
}

// updateLastIteratedWithCustomValues updates lastIterated with the current
// value of the custom field in it.customFields at index i. This ensures that
// when we return it.lastIterated in the call to Current() that all the
// most recent values are present.
func (it *iterator) updateLastIteratedWithCustomValues(arg updateLastIterArg) error {
	// TODO: Can delete this?
	if it.lastIterated == nil {
		it.lastIterated = dynamic.NewMessage(it.schema)
	}

	var (
		fieldNum  = int32(it.customFields[arg.i].fieldNum)
		fieldType = it.customFields[arg.i].fieldType
	)

	// TODO: Can delete this?
	if field := it.schema.FindFieldByNumber(fieldNum); field == nil {
		// This can happen when the field being decoded does not exist (or is reserved)
		// in the current schema, but the message was encoded with a schema in which the
		// field number did exist.
		return nil
	}

	switch {
	case isCustomFloatEncodedField(fieldType):
		var (
			val = math.Float64frombits(it.customFields[arg.i].floatEncAndIter.PrevFloatBits)
			err error
		)
		if fieldType == float64Field {
			it.marshaler.encFloat64(fieldNum, val)
		} else {
			it.marshaler.encFloat32(fieldNum, float32(val))
		}
		return err

	case isCustomIntEncodedField(fieldType):
		switch fieldType {
		case signedInt64Field:
			var (
				val   = int64(it.customFields[arg.i].intEncAndIter.prevIntBits)
				field = it.schema.FindFieldByNumber(fieldNum)
			)
			if field == nil {
				return fmt.Errorf(
					"updating last iterated with value, could not find field number %d in schema", fieldNum)
			}

			fieldType := field.GetType()
			if fieldType == dpb.FieldDescriptorProto_TYPE_SINT64 {
				// The encoding / compression schema in this package treats Protobuf int32 and sint32 the same,
				// however, Protobuf unmarshalers assume that fields of type sint are zigzag. As a result, the
				// iterator needs to check the fields protobuf type so that it can perform the correct encoding.
				it.marshaler.encSInt64(fieldNum, val)
			} else if fieldType == dpb.FieldDescriptorProto_TYPE_SFIXED64 {
				it.marshaler.encSFixedInt64(fieldNum, val)
			} else {
				it.marshaler.encInt64(fieldNum, val)
			}
			return nil

		case unsignedInt64Field:
			val := it.customFields[arg.i].intEncAndIter.prevIntBits
			it.marshaler.encUInt64(fieldNum, val)
			return nil

		case signedInt32Field:
			var (
				val   = int32(it.customFields[arg.i].intEncAndIter.prevIntBits)
				field = it.schema.FindFieldByNumber(fieldNum)
			)
			if field == nil {
				return fmt.Errorf(
					"updating last iterated with value, could not find field number %d in schema", fieldNum)
			}

			fieldType := field.GetType()
			if fieldType == dpb.FieldDescriptorProto_TYPE_SINT32 {
				// The encoding / compression schema in this package treats Protobuf int32 and sint32 the same,
				// however, Protobuf unmarshalers assume that fields of type sint are zigzag. As a result, the
				// iterator needs to check the fields protobuf type so that it can perform the correct encoding.
				it.marshaler.encSInt32(fieldNum, val)
			} else if fieldType == dpb.FieldDescriptorProto_TYPE_SFIXED32 {
				it.marshaler.encSFixedInt32(fieldNum, val)
			} else {
				it.marshaler.encInt32(fieldNum, val)
			}
			return nil

		case unsignedInt32Field:
			val := uint32(it.customFields[arg.i].intEncAndIter.prevIntBits)
			it.marshaler.encUInt32(fieldNum, val)
			return nil

		default:
			return fmt.Errorf(
				"%s expected custom int encoded field but field type was: %v",
				itErrPrefix, fieldType)
		}

	case fieldType == bytesField:
		it.marshaler.encBytes(fieldNum, arg.bytesFieldBuf)
		return nil

	case fieldType == boolField:
		it.marshaler.encBool(fieldNum, arg.boolVal)
		return nil

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

func (it *iterator) lastValueBytesDict(fieldIdx int) []byte {
	dict := it.customFields[fieldIdx].iteratorBytesFieldDict
	return dict[len(dict)-1]
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
	it.unmarshalProtoBuf = it.newBuffer(n)
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

func (it *iterator) newBuffer(capacity int) checked.Bytes {
	if bytesPool := it.opts.BytesPool(); bytesPool != nil {
		return bytesPool.Get(capacity)
	}
	return checked.NewBytes(make([]byte, 0, capacity), nil)
}
