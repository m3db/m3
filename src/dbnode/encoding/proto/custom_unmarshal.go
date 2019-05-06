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
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

var (
	// Groups in the Protobuf wire format are deprecated, so simplify the code significantly by
	// not supporting them.
	errGroupsAreNotSupported = errors.New("use of groups in proto wire format is not supported")
	zeroValue                unmarshalValue
)

type customFieldUnmarshaler interface {
	sortedCustomFieldValues() sortedCustomFieldValues
	nonCustomFieldValues() *dynamic.Message
	numNonCustomValues() int
	resetAndUnmarshal(schema *desc.MessageDescriptor, buf []byte) error
}

type customUnmarshaler struct {
	schema *desc.MessageDescriptor

	decodeBuf *buffer

	customValues sortedCustomFieldValues

	nonCustomValues *dynamic.Message
	numNonCustom    int
}

func newCustomFieldUnmarshaler() customFieldUnmarshaler {
	return &customUnmarshaler{
		decodeBuf: newCodedBuffer(nil),
	}
}

func (u *customUnmarshaler) sortedCustomFieldValues() sortedCustomFieldValues {
	return u.customValues
}

func (u *customUnmarshaler) numNonCustomValues() int {
	return u.numNonCustom
}

func (u *customUnmarshaler) nonCustomFieldValues() *dynamic.Message {
	if u.nonCustomValues == nil {
		u.nonCustomValues = dynamic.NewMessage(u.schema)
	}
	return u.nonCustomValues
}

func (u *customUnmarshaler) unmarshal() error {
	u.customValues = u.customValues[:0]

	if u.nonCustomValues != nil {
		u.nonCustomValues.Reset()
	}

	isSorted := true
	for !u.decodeBuf.eof() {
		tagAndWireTypeStartOffset := u.decodeBuf.index
		fieldNum, wireType, err := u.decodeBuf.decodeTagAndWireType()
		if err != nil {
			return err
		}

		fd := u.schema.FindFieldByNumber(fieldNum)
		if fd == nil {
			return fmt.Errorf("encountered unknown field with field number: %d", fieldNum)
		}

		isCustomField := u.isCustomField(fd)
		if !isCustomField {
			if u.nonCustomValues == nil {
				u.nonCustomValues = dynamic.NewMessage(u.schema)
			}

			_, err = u.skip(wireType)
			if err != nil {
				return err
			}

			var (
				length   = u.decodeBuf.index - tagAndWireTypeStartOffset
				startIdx = tagAndWireTypeStartOffset
				endIdx   = startIdx + length
			)
			u.nonCustomValues.UnmarshalMerge(u.decodeBuf.buf[tagAndWireTypeStartOffset:endIdx])
			u.numNonCustom++
			continue
		}

		value, err := u.unmarshalKnownField(fd, wireType)
		if err != nil {
			return err
		}

		if isSorted && len(u.customValues) > 1 {
			// Check if the slice is sorted as its built to avoid resorting
			// unnecessarily at the end.
			lastFieldNum := u.customValues[len(u.customValues)-1].fieldNumber
			if fieldNum < lastFieldNum {
				isSorted = false
			}
		}

		u.customValues = append(u.customValues, value)
	}

	u.decodeBuf.reset(u.decodeBuf.buf)

	if !isSorted {
		// Avoid resorting if possible.
		sort.Sort(u.customValues)
	}

	return nil
}

func (u *customUnmarshaler) isCustomField(fd *desc.FieldDescriptor) bool {
	if fd.IsRepeated() || fd.IsMap() {
		// Map should always be repeated but include the guard just in case.
		return false
	}

	if fd.GetMessageType() != nil {
		// Skip nested messages.
		return false
	}

	return true
}

// skip will skip over the next value in the encoded stream (given that the tag and
// wiretype have already been decoded). Additionally, it can optionally re-encode
// the skipped <tag,wireType,value> tuple into the skippedBuf stream so that it can
// be handled later.
func (u *customUnmarshaler) skip(wireType int8) (int, error) {
	switch wireType {
	case proto.WireFixed32:
		numSkipped := 4
		u.decodeBuf.index += numSkipped
		return numSkipped, nil

	case proto.WireFixed64:
		numSkipped := 8
		u.decodeBuf.index += numSkipped
		return numSkipped, nil

	case proto.WireVarint:
		var (
			numSkipped               = 0
			offsetBeforeDecodeVarInt = u.decodeBuf.index
		)
		_, err := u.decodeBuf.decodeVarint()
		if err != nil {
			return numSkipped, err
		}
		numSkipped += u.decodeBuf.index - offsetBeforeDecodeVarInt
		return numSkipped, nil

	case proto.WireBytes:
		var (
			numSkipped                 = 0
			offsetBeforeDecodeRawBytes = u.decodeBuf.index
		)
		// Bytes aren't copied because they're just being skipped over so
		// copying would be wasteful.
		_, err := u.decodeBuf.decodeRawBytes(false)
		if err != nil {
			return numSkipped, err
		}
		numSkipped += u.decodeBuf.index - offsetBeforeDecodeRawBytes
		return numSkipped, nil

	case proto.WireStartGroup:
		return 0, errGroupsAreNotSupported
	case proto.WireEndGroup:
		return 0, errGroupsAreNotSupported
	default:
		return 0, proto.ErrInternalBadWireType
	}
}

func (u *customUnmarshaler) unmarshalKnownField(fd *desc.FieldDescriptor, wireType int8) (unmarshalValue, error) {
	switch wireType {
	case proto.WireFixed32:
		num, err := u.decodeBuf.decodeFixed32()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)
	case proto.WireFixed64:
		num, err := u.decodeBuf.decodeFixed64()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)
	case proto.WireVarint:
		num, err := u.decodeBuf.decodeVarint()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)

	case proto.WireBytes:
		if fd.GetType() != dpb.FieldDescriptorProto_TYPE_BYTES &&
			fd.GetType() != dpb.FieldDescriptorProto_TYPE_STRING {
			// This should never happen since it means the skipping logic is not working
			// correctly or the message is malformed since proto.WireBytes should only be
			// used for fields of type bytes, string, group, or message. Groups/messages
			// should be handled by the skipping logic (for now).
			return zeroValue, fmt.Errorf(
				"tried to unmarshal field with wire type: bytes and proto field type: %s",
				fd.GetType().String())
		}

		// Don't bother copying the bytes now because the encoder has exclusive ownership
		// of them until the call to Encode() completes and they will get "copied" anyways
		// once they're written into the OStream.
		raw, err := u.decodeBuf.decodeRawBytes(false)
		if err != nil {
			return zeroValue, err
		}

		val := unmarshalValue{fieldNumber: fd.GetNumber(), bytes: raw}
		return val, nil

	case proto.WireStartGroup:
		return zeroValue, errGroupsAreNotSupported
	default:
		return zeroValue, proto.ErrInternalBadWireType
	}
}

func unmarshalSimpleField(fd *desc.FieldDescriptor, v uint64) (unmarshalValue, error) {
	val := unmarshalValue{fieldNumber: fd.GetNumber(), v: v}
	switch fd.GetType() {
	case dpb.FieldDescriptorProto_TYPE_BOOL,
		dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_FIXED64,
		dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_SFIXED64,
		dpb.FieldDescriptorProto_TYPE_DOUBLE:
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_UINT32,
		dpb.FieldDescriptorProto_TYPE_FIXED32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_ENUM:
		s := int64(v)
		if s > math.MaxInt32 || s < math.MinInt32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SFIXED32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SINT32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.v = uint64(decodeZigZag32(v))
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SINT64:
		val.v = uint64(decodeZigZag64(v))
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		float32Val := math.Float32frombits(uint32(v))
		float64Bits := math.Float64bits(float64(float32Val))
		val.v = float64Bits
		return val, nil

	default:
		// bytes, string, message, and group cannot be represented as a simple numeric value.
		return zeroValue, fmt.Errorf("bad input; field %s requires length-delimited wire type", fd.GetFullyQualifiedName())
	}
}

func (u *customUnmarshaler) resetAndUnmarshal(schema *desc.MessageDescriptor, buf []byte) error {
	if schema != u.schema {
		u.nonCustomValues = dynamic.NewMessage(schema)
		u.nonCustomValues.Reset()
	}

	u.schema = schema
	u.numNonCustom = 0
	u.customValues = u.customValues[:0]
	u.decodeBuf.reset(buf)

	return u.unmarshal()
}

type sortedCustomFieldValues []unmarshalValue

func (s sortedCustomFieldValues) Len() int {
	return len(s)
}

func (s sortedCustomFieldValues) Less(i, j int) bool {
	return s[i].fieldNumber < s[j].fieldNumber
}

func (s sortedCustomFieldValues) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type unmarshalValue struct {
	fieldNumber int32
	v           uint64
	bytes       []byte
}

func (v *unmarshalValue) asBool() bool {
	return v.v != 0
}

func (v *unmarshalValue) asUint64() uint64 {
	return v.v
}

func (v *unmarshalValue) asInt64() int64 {
	return int64(v.v)
}

func (v *unmarshalValue) asFloat64() float64 {
	return math.Float64frombits(v.v)
}

func (v *unmarshalValue) asBytes() []byte {
	return v.bytes
}
