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

	"github.com/golang/snappy"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
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

	hasWrittenFirstTSZ bool
	closed             bool
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

	enc.hasWrittenFirstTSZ = false
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

		customEncoded := false
		if customField.fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE {
			enc.encodeTSZValue(i, customField, iVal)
			customEncoded = true
		}
		// } else if customField.fieldType == dpb.FieldDescriptorProto_TYPE_BYTES {
		// 	enc.encodeBytesValue(i, customField, iVal)
		// 	customEncoded = true
		// }

		if customEncoded {
			// Remove the field from the message so we don't include it
			// in the proto marshal.
			m.ClearFieldByNumber(customField.fieldNum)
		}
	}
	enc.hasWrittenFirstTSZ = true

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

	if !enc.hasWrittenFirstTSZ {
		enc.encodeFirstTSZValue(i, val)
	} else {
		enc.encodeNextTSZValue(i, val)
	}

	return nil
}

func (enc *encoder) encodeIntValue(i int, customField customFieldState, iVal interface{}) error {
	var val uint64
	switch typedVal := iVal.(type) {
	case int64:
		val = uint64(typedVal)
	default:
		return fmt.Errorf(
			"proto encoder: found unknown type in fieldNum %d", customField.fieldNum)
	}

	if !enc.hasWrittenFirstTSZ {
		enc.encodeFirstIntValue(i, val)
	} else {
		enc.encodeNextIntValue(i, val)
	}

	return nil
}

func (enc *encoder) encodeBytesValue(i int, customField customFieldState, iVal interface{}) error {
	currBytes, ok := iVal.([]byte)
	if !ok {
		return fmt.Errorf(
			"proto encoder: found unknown type in fieldNum %d", customField.fieldNum)
	}

	if bytes.Equal(customField.prevBytes, currBytes) {
		// No changes control bit.
		enc.stream.WriteBit(0)
		return nil
	}

	if customField.bytesWriter == nil {
		customField.bytesWriter = snappy.NewWriter(enc.stream)
		enc.customFields[i] = customField
	}

	// Bytes changed control bit.
	enc.stream.WriteBit(1)
	enc.encodeVarInt((uint64(len(currBytes))))
	n, err := customField.bytesWriter.Write(currBytes)
	if err != nil {
		return fmt.Errorf("proto encoder: error snappy compressing bytes: %v", err)
	}
	if n != len(currBytes) {
		return fmt.Errorf(
			"proto encoder: tried to snappy compress %d bytes but only compressed: %d",
			len(currBytes), n)
	}
	if err := customField.bytesWriter.Flush(); err != nil {
		// TODO: Not sure if this is necessary?
		return fmt.Errorf(
			"proto encoder: error flushing snappy writer: %v", err)
	}
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

func (enc *encoder) encodeFirstIntValue(i int, v uint64) {
	enc.customFields[i].prevFloatBits = v
}

func (enc *encoder) encodeNextIntValue(i int, next uint64) {

}

func writeIntValDiff(stream encoding.OStream, valBits uint64, neg bool, numSig uint8) {
	if neg {
		// opCodeNegative
		stream.WriteBit(0x1)
	} else {
		// opCodePositive
		stream.WriteBit(0x0)
	}

	stream.WriteBits(valBits, int(numSig))
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
