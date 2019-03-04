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
	"math"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3x/checked"
)

var (
	errSchemaIsRequired           = errors.New("proto encoder: schema is required")
	errEncodingOptionsAreRequired = errors.New("proto encoder: encoding options are required")
	errMessageHasUnknownFields    = errors.New("proto encoder: message has unknown fields")
)

type encoder struct {
	stream             encoding.OStream
	schema             *desc.MessageDescriptor
	hasWrittenFirstTSZ bool

	lastEncoded *dynamic.Message
	tszFields   []tszFieldState

	// Fields that are reused between function calls to
	// avoid allocations.
	varIntBuf     [8]byte
	changesValues []int32
}

type tszFieldState struct {
	fieldNum      int
	prevXOR       uint64
	prevFloatBits uint64
}

// NewEncoder creates a new encoder.
func NewEncoder(
	b checked.Bytes,
	schema *desc.MessageDescriptor,
	opts encoding.Options,
) (*encoder, error) {
	if schema == nil {
		return nil, errSchemaIsRequired
	}
	if opts == nil {
		return nil, errEncodingOptionsAreRequired
	}

	initAllocIfEmpty := opts.EncoderPool() == nil
	enc := &encoder{
		// TODO: Pass in options, use pooling, etc.
		stream:    encoding.NewOStream(b, initAllocIfEmpty, opts.BytesPool()),
		schema:    schema,
		tszFields: tszFields(nil, schema),
		varIntBuf: [8]byte{},
	}

	return enc, nil
}

// TODO: Add concept of hard/soft error and if there is a hard error
// then the encoder cant be used anymore.
func (enc *encoder) Encode(m *dynamic.Message) error {
	if len(m.GetUnknownFields()) > 0 {
		return errMessageHasUnknownFields
	}

	if err := enc.encodeTSZValues(m); err != nil {
		return err
	}
	if err := enc.encodeProtoValues(m); err != nil {
		return err
	}

	return nil
}

func (enc *encoder) encodeTSZValues(m *dynamic.Message) error {
	for i, tszField := range enc.tszFields {
		iVal, err := m.TryGetFieldByNumber(tszField.fieldNum)
		if err != nil {
			return fmt.Errorf(
				"proto encoder error trying to get field number: %d",
				tszField.fieldNum)
		}

		var val float64
		if typedVal, ok := iVal.(float64); ok {
			val = typedVal
		} else {
			// TODO: Better error handling here
			val = float64(iVal.(float32))
		}

		if !enc.hasWrittenFirstTSZ {
			enc.writeFirstTSZValue(i, val)
		} else {
			enc.writeNextTSZValue(i, val)
		}

		// Remove the field from the message so we don't include it
		// in the proto marshal.
		m.ClearFieldByNumber(tszField.fieldNum)
	}
	enc.hasWrittenFirstTSZ = true

	return nil
}

func (enc *encoder) encodeProtoValues(m *dynamic.Message) error {
	// Reset for re-use.
	enc.changesValues = enc.changesValues[:0]
	changedFields := enc.changesValues

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

	marshaled, err := m.Marshal()
	if err != nil {
		return fmt.Errorf("proto encoder error trying to marshal protobuf: %v", err)
	}

	enc.stream.WriteBit(1)
	enc.writeBitset(changedFields)
	enc.writeVarInt(uint64(len(marshaled)))
	enc.stream.WriteBytes(marshaled)

	if enc.lastEncoded == nil {
		// Set lastEncoded to m so that subsequent encoding only need to encode fields
		// that have changed.
		enc.lastEncoded = m
	} else {
		// lastEncoded has already been mutated to reflect the current state.
	}

	return nil
}

func (enc *encoder) writeFirstTSZValue(i int, v float64) {
	fb := math.Float64bits(v)
	enc.stream.WriteBits(fb, 64)
	enc.tszFields[i].prevFloatBits = fb
	enc.tszFields[i].prevXOR = fb
}

func (enc *encoder) writeNextTSZValue(i int, next float64) {
	curFloatBits := math.Float64bits(next)
	curXOR := enc.tszFields[i].prevFloatBits ^ curFloatBits
	m3tsz.WriteXOR(enc.stream, enc.tszFields[i].prevXOR, curXOR)
	enc.tszFields[i].prevFloatBits = curFloatBits
	enc.tszFields[i].prevXOR = curXOR
}

func (enc *encoder) writeBitset(values []int32) {
	var max int32
	for _, v := range values {
		if v > max {
			max = v
		}
	}

	// Encode a varint that indicates how many of the remaining
	// bits to interpret as a bitset.
	enc.writeVarInt(uint64(max))

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

func (enc *encoder) writeVarInt(x uint64) {
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
