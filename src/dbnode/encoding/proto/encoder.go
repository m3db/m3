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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	murmur3 "github.com/m3db/stackmurmur3"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3x/checked"
)

const (
	// Maximum capacity of a slice of TSZ fields that will be retained between resets.
	maxTSZFieldsCapacityRetain = 24
)

var (
	errEncoderSchemaIsRequired           = errors.New("proto encoder: schema is required")
	errEncoderEncodingOptionsAreRequired = errors.New("proto encoder: encoding options are required")
	errEncoderMessageHasUnknownFields    = errors.New("proto encoder: message has unknown fields")
	errEncoderClosed                     = errors.New("proto encoder: encoder is closed")
)

// TODO(rartoul): Need to support schema changes by updating the ordering
// of the TSZ encoded fields on demand.
// TODO: Encode the LRU size into the stream
type encoder struct {
	stream encoding.OStream
	schema *desc.MessageDescriptor

	lastEncoded  *dynamic.Message
	customFields []customFieldState

	// Fields that are reused between function calls to
	// avoid allocations.
	varIntBuf              [8]byte
	changedValues          []int32
	fieldsChangedToDefault []int32

	hasEncodedFirstSetOfCustomValues bool
	closed                           bool
}

// NewEncoder creates a new encoder.
func NewEncoder(opts encoding.Options) (*encoder, error) {
	if opts == nil {
		return nil, errEncoderEncodingOptionsAreRequired
	}

	initAllocIfEmpty := opts.EncoderPool() == nil
	enc := &encoder{
		stream:    encoding.NewOStream(nil, initAllocIfEmpty, opts.BytesPool()),
		varIntBuf: [8]byte{},
	}

	return enc, nil
}

// TODO: Add concept of hard/soft error and if there is a hard error
// then the encoder cant be used anymore.
func (enc *encoder) Encode(m *dynamic.Message) error {
	if enc.closed {
		return errEncoderClosed
	}
	if enc.schema == nil {
		return errEncoderSchemaIsRequired
	}

	if len(m.GetUnknownFields()) > 0 {
		return errEncoderMessageHasUnknownFields
	}

	// Control bit that indicates the stream has more data.
	enc.stream.WriteBit(1)

	if err := enc.encodeCustomValues(m); err != nil {
		return err
	}
	if err := enc.encodeProtoValues(m); err != nil {
		return err
	}

	return nil
}

func (enc *encoder) Reset(
	b checked.Bytes,
	schema *desc.MessageDescriptor,
) {
	enc.stream.Reset(b)
	enc.schema = schema
	enc.lastEncoded = nil
	if cap(enc.customFields) <= maxTSZFieldsCapacityRetain {
		enc.customFields = customFields(enc.customFields, schema)
	} else {
		enc.customFields = customFields(nil, schema)
	}

	enc.hasEncodedFirstSetOfCustomValues = false
	enc.closed = false
}

func (enc *encoder) Bytes() (checked.Bytes, error) {
	if enc.closed {
		return nil, errEncoderClosed
	}

	bytes, _ := enc.stream.Rawbytes()
	return bytes, nil
}

func (enc *encoder) Close() (checked.Bytes, error) {
	if enc.closed {
		return nil, errEncoderClosed
	}

	bytes := enc.stream.Discard()
	enc.closed = true
	return bytes, nil
}

func (enc *encoder) encodeCustomValues(m *dynamic.Message) error {
	for i, customField := range enc.customFields {
		iVal, err := m.TryGetFieldByNumber(customField.fieldNum)
		if err != nil {
			return fmt.Errorf(
				"proto encoder error trying to get field number: %d",
				customField.fieldNum)
		}

		customEncoded := true
		switch {
		case isCustomFloatEncodedField(customField.fieldType):
			if err := enc.encodeTSZValue(i, customField, iVal); err != nil {
				return err
			}
		case isCustomIntEncodedField(customField.fieldType):
			if err := enc.encodeIntValue(i, customField, iVal); err != nil {
				return err
			}
		case customField.fieldType == cBytes:
			if err := enc.encodeBytesValue(i, customField, iVal); err != nil {
				return err
			}
		default:
			customEncoded = false
		}

		if customEncoded {
			// Remove the field from the message so we don't include it
			// in the proto marshal.
			m.ClearFieldByNumber(customField.fieldNum)
		}
	}
	enc.hasEncodedFirstSetOfCustomValues = true

	return nil
}

func (enc *encoder) encodeTSZValue(i int, customField customFieldState, iVal interface{}) error {
	var val float64
	switch typedVal := iVal.(type) {
	case float64:
		val = typedVal
	case float32:
		val = float64(typedVal)
	default:
		return fmt.Errorf(
			"proto encoder: found unknown type in fieldNum %d", customField.fieldNum)
	}

	if !enc.hasEncodedFirstSetOfCustomValues {
		enc.encodeFirstTSZValue(i, val)
	} else {
		enc.encodeNextTSZValue(i, val)
	}

	return nil
}

func (enc *encoder) encodeIntValue(i int, customField customFieldState, iVal interface{}) error {
	var (
		signedVal   int64
		unsignedVal uint64
	)
	switch typedVal := iVal.(type) {
	case uint64:
		unsignedVal = typedVal
	case uint32:
		unsignedVal = uint64(typedVal)
	case int64:
		signedVal = typedVal
	case int32:
		signedVal = int64(typedVal)
	default:
		return fmt.Errorf(
			"proto encoder: found unknown type in fieldNum %d", customField.fieldNum)
	}

	if isUnsignedInt(customField.fieldType) {
		if !enc.hasEncodedFirstSetOfCustomValues {
			enc.encodeFirstUnsignedIntValue(i, unsignedVal)
		} else {
			enc.encodeNextUnsignedIntValue(i, unsignedVal)
		}
	} else {
		if !enc.hasEncodedFirstSetOfCustomValues {
			enc.encodeFirstSignedIntValue(i, signedVal)
		} else {
			enc.encodeNextSignedIntValue(i, signedVal)
		}
	}

	return nil
}

func (enc *encoder) encodeBytesValue(i int, customField customFieldState, iVal interface{}) error {
	currBytes, ok := iVal.([]byte)
	if !ok {
		currString, ok := iVal.(string)
		if !ok {
			return fmt.Errorf(
				"proto encoder: found unknown type in fieldNum %d", customField.fieldNum)
		}
		currBytes = []byte(currString)
	}

	if bytes.Equal(customField.prevBytes, currBytes) {
		// No changes control bit.
		enc.stream.WriteBit(0)
		return nil
	}

	// Bytes changed control bit.
	enc.stream.WriteBit(1)

	hash := murmur3.Sum64(currBytes)
	for j, prevHash := range customField.bytesFieldDict {
		if hash == prevHash {
			// Control bit means interpret next n bits as the index for the previous write
			// that this matches where n is the number of bits required to represent all
			// possible array indices in the configured LRU size.
			enc.stream.WriteBit(0)
			enc.stream.WriteBits(uint64(j), numBitsRequiredToRepresentArrayIndex(byteFieldDictSize))
			enc.moveToEndOfBytesDict(i, j)
			enc.customFields[i].prevBytes = currBytes
			return nil
		}
	}

	// Control bit means interpret subsequent bits as varInt encoding length of a new
	// []byte we haven't seen before.
	enc.stream.WriteBit(1)
	enc.encodeVarInt((uint64(len(currBytes))))
	for _, b := range currBytes {
		enc.stream.WriteByte(b)
	}
	enc.addToBytesDict(i, hash)
	enc.customFields[i].prevBytes = currBytes
	return nil
}

func (enc *encoder) encodeProtoValues(m *dynamic.Message) error {
	// Reset for re-use.
	enc.changedValues = enc.changedValues[:0]
	changedFields := enc.changedValues

	enc.fieldsChangedToDefault = enc.fieldsChangedToDefault[:0]
	fieldsChangedToDefault := enc.fieldsChangedToDefault

	if enc.lastEncoded != nil {
		schemaFields := enc.schema.GetFields()
		for _, field := range m.GetKnownFields() {
			// Clear out any fields that were provided but are not in the schema
			// to prevent wasting space on them.
			// TODO(rartoul):This is an uncommon scenario and this operation might
			// be expensive to perform each time so consider removing this if it
			// impacts performance too much.
			fieldNum := field.GetNumber()
			if !fieldsContains(fieldNum, schemaFields) {
				if err := m.TryClearFieldByNumber(int(fieldNum)); err != nil {
					return err
				}
			}
		}

		for _, field := range schemaFields {
			var (
				fieldNum    = field.GetNumber()
				fieldNumInt = int(fieldNum)
				prevVal     = enc.lastEncoded.GetFieldByNumber(fieldNumInt)
				curVal      = m.GetFieldByNumber(fieldNumInt)
			)

			if fieldsEqual(curVal, prevVal) {
				// Clear fields that haven't changed.
				if err := m.TryClearFieldByNumber(fieldNumInt); err != nil {
					return err
				}
			} else {
				if fieldsEqual(field.GetDefaultValue(), curVal) {
					fieldsChangedToDefault = append(fieldsChangedToDefault, fieldNum)
				}
				changedFields = append(changedFields, fieldNum)
				if err := enc.lastEncoded.TrySetFieldByNumber(fieldNumInt, curVal); err != nil {
					return err
				}
			}
		}
	}

	if len(changedFields) == 0 && enc.lastEncoded != nil {
		// Only want to skip encoding if nothing has changed AND we've already
		// encoded the first message.
		enc.stream.WriteBit(0)
		return nil
	}

	// TODO: Probably need to add a MarshalInto() in the underlying library.
	marshaled, err := m.Marshal()
	if err != nil {
		return fmt.Errorf("proto encoder error trying to marshal protobuf: %v", err)
	}

	// Control bit indicating that proto values have changed.
	enc.stream.WriteBit(1)
	if len(fieldsChangedToDefault) > 0 {
		// Control bit indicating that some fields have been set to default values
		// and that a bitset will follow specifying which fields have changed.
		enc.stream.WriteBit(1)
		enc.encodeBitset(fieldsChangedToDefault)
	} else {
		// Control bit indicating that none of the changed fields have been set to
		// their default values so we can do a clean merge on read.
		enc.stream.WriteBit(0)
	}
	enc.encodeVarInt(uint64(len(marshaled)))
	enc.stream.WriteBytes(marshaled)

	if enc.lastEncoded == nil {
		// Set lastEncoded to m so that subsequent encodings only need to encode fields
		// that have changed.
		enc.lastEncoded = m
	} else {
		// lastEncoded has already been mutated to reflect the current state.
	}

	return nil
}

func (enc *encoder) encodeFirstTSZValue(i int, v float64) {
	fb := math.Float64bits(v)
	enc.stream.WriteBits(fb, 64)
	enc.customFields[i].prevFloatBits = fb
	enc.customFields[i].prevXOR = fb
}

func (enc *encoder) encodeNextTSZValue(i int, next float64) {
	curFloatBits := math.Float64bits(next)
	curXOR := enc.customFields[i].prevFloatBits ^ curFloatBits
	m3tsz.WriteXOR(enc.stream, enc.customFields[i].prevXOR, curXOR)
	enc.customFields[i].prevFloatBits = curFloatBits
	enc.customFields[i].prevXOR = curXOR
}

func (enc *encoder) encodeFirstSignedIntValue(i int, v int64) {
	neg := false
	enc.customFields[i].prevFloatBits = uint64(v)
	if v < 0 {
		neg = true
		v = -1 * v
	}

	vBits := uint64(v)
	numSig := encoding.NumSig(vBits)

	enc.encodeIntSig(i, numSig)
	enc.encodeIntValDiff(vBits, neg, numSig)
}

func (enc *encoder) encodeFirstUnsignedIntValue(i int, v uint64) {
	enc.customFields[i].prevFloatBits = uint64(v)

	vBits := uint64(v)
	numSig := encoding.NumSig(vBits)

	enc.encodeIntSig(i, numSig)
	enc.encodeIntValDiff(vBits, false, numSig)
}

func (enc *encoder) encodeNextSignedIntValue(i int, next int64) {
	prev := int64(enc.customFields[i].prevFloatBits)
	diff := next - prev
	if diff == 0 {
		// NoChangeControlBit
		enc.stream.WriteBit(0)
		return
	}

	enc.stream.WriteBit(1)

	neg := false
	if diff < 0 {
		neg = true
		diff = -1 * diff
	}

	diffBits := uint64(diff)
	numSig := encoding.NumSig(diffBits)
	// TODO: newSig tracking bullshit
	enc.encodeIntSig(i, numSig)
	enc.encodeIntValDiff(diffBits, neg, numSig)
	enc.customFields[i].prevFloatBits = uint64(next)
}

func (enc *encoder) encodeNextUnsignedIntValue(i int, next uint64) {
	var (
		neg  = false
		prev = uint64(enc.customFields[i].prevFloatBits)
		diff uint64
	)

	// Avoid overflows.
	if next > prev {
		diff = next - prev
	} else {
		neg = true
		diff = prev - next
	}

	if diff == 0 {
		// NoChangeControlBit
		enc.stream.WriteBit(0)
		return
	}

	enc.stream.WriteBit(1)

	numSig := encoding.NumSig(diff)
	// TODO: newSig tracking bullshit
	enc.encodeIntSig(i, numSig)
	enc.encodeIntValDiff(diff, neg, numSig)
	enc.customFields[i].prevFloatBits = uint64(next)
}

func (enc *encoder) encodeIntSig(i int, currSig uint8) {
	prevSig := enc.customFields[i].prevSig
	if currSig != prevSig {
		// opcodeUpdateSig
		enc.stream.WriteBit(0x1)
		if currSig == 0 {
			// opcodeZeroSig
			enc.stream.WriteBit(0x0)
		} else {
			// opcodeNonZeroSig
			enc.stream.WriteBit(0x1)
			enc.stream.WriteBits(uint64(currSig-1), 6) // 2^6 == 64
		}
	} else {
		// opcodeNoUpdateSig
		enc.stream.WriteBit(0x0)
	}

	enc.customFields[i].prevSig = currSig
}

func (enc *encoder) encodeIntValDiff(valBits uint64, neg bool, numSig uint8) {
	if neg {
		// opCodeNegative
		enc.stream.WriteBit(0x1)
	} else {
		// opCodePositive
		enc.stream.WriteBit(0x0)
	}

	enc.stream.WriteBits(valBits, int(numSig))
}

func (enc *encoder) moveToEndOfBytesDict(fieldIdx, i int) {
	existing := enc.customFields[fieldIdx].bytesFieldDict
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

func (enc *encoder) addToBytesDict(fieldIdx int, hash uint64) {
	existing := enc.customFields[fieldIdx].bytesFieldDict
	if len(existing) < byteFieldDictSize {
		enc.customFields[fieldIdx].bytesFieldDict = append(existing, hash)
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

	existing[len(existing)-1] = hash
}

func (enc *encoder) bytes(i int, next float64) checked.Bytes {
	bytes, _ := enc.stream.Rawbytes()
	return bytes
}

// encodeBitset writes out a bitset in the form of:
//
//      varint(number of bits)|bitset
//
// I.E first it encodes a varint which specifies the number of following
// bits to interpret as a bitset and then it encodes the provided values
// as zero-indexed bitset.
func (enc *encoder) encodeBitset(values []int32) {
	var max int32
	for _, v := range values {
		if v > max {
			max = v
		}
	}

	// Encode a varint that indicates how many of the remaining
	// bits to interpret as a bitset.
	enc.encodeVarInt(uint64(max))

	// Encode the bitset
	for i := int32(0); i < max; i++ {
		wroteExists := false

		for _, v := range values {
			// Subtract one because the values are 1-indexed but the bitset
			// is 0-indexed.
			if i == v-1 {
				enc.stream.WriteBit(1)
				wroteExists = true
				break
			}
		}

		if wroteExists {
			continue
		}

		enc.stream.WriteBit(0)
	}
}

func (enc *encoder) encodeVarInt(x uint64) {
	var (
		// Convert array to slice we can reuse the buffer.
		buf      = enc.varIntBuf[:]
		numBytes = binary.PutUvarint(buf, x)
	)

	// Reslice so we only write out as many bytes as is required
	// to represent the number.
	buf = buf[:numBytes]
	enc.stream.WriteBytes(buf)
}
