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
	// ErrOverflow is returned when an integer is too large to be represented.
	ErrOverflow = errors.New("proto: integer overflow")

	// Groups in the Protobuf wire format are deprecated, so simplify the code significantly by
	// not supporting them.
	errGroupsAreNotSupported = errors.New("use of groups in proto wire format is not supported")

	errMessagesShouldNotBeIterativelyUnmarshaled = errors.New("messages should not be iteratively unmarshaled")

	zeroValue unmarshalValue
)

type sortedUnmarshalIter struct {
	schema *desc.MessageDescriptor

	decodeBuf *buffer

	calculateSortedOffsetsComplete bool

	last unmarshalValue

	sortedOffsets    sortedOffsets
	sortedOffsetsIdx int

	skippedOffsets skippedOffsets

	skippedMessage *dynamic.Message
	skippedCount   int
	e              error
}

// Implement Sort interface because sort.Slice allocates.
type sortedOffsets []sortedOffset

type skippedOffsets []skippedOffset

type sortedOffset struct {
	offset   int
	fd       *desc.FieldDescriptor
	wireType int8
}

type skippedOffset struct {
	offset int
	length int
}

func (s sortedOffsets) Len() int {
	return len(s)
}

func (s sortedOffsets) Less(i, j int) bool {
	return s[i].offset < s[j].offset
}

func (s sortedOffsets) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type unmarshalValue struct {
	fd    *desc.FieldDescriptor
	v     uint64
	bytes []byte
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

func newUnmarshalIter() *sortedUnmarshalIter {
	return &sortedUnmarshalIter{
		decodeBuf: newCodedBuffer(nil),
	}
}

func (u *sortedUnmarshalIter) next() bool {
	if !u.calculateSortedOffsetsComplete {
		err := u.calculateSortedOffsets()
		if err != nil {
			u.e = err
			return false
		}
	}

	if !u.hasNext() {
		return false
	}

	err := u.unmarshalField()
	if err != nil {
		u.e = err
		return false
	}

	return true
}

func (u *sortedUnmarshalIter) current() unmarshalValue {
	return u.last
}

func (u *sortedUnmarshalIter) numSkipped() int {
	return u.skippedCount
}

func (u *sortedUnmarshalIter) skipped() (*dynamic.Message, error) {
	if u.skippedMessage == nil {
		u.skippedMessage = dynamic.NewMessage(u.schema)
	}
	u.skippedMessage.Reset()
	for _, o := range u.skippedOffsets {
		err := u.skippedMessage.UnmarshalMerge(u.decodeBuf.buf[o.offset : o.offset+o.length])
		if err != nil {
			return nil, err
		}
	}
	return u.skippedMessage, nil
}

// calculateSortedOffsets calculates the sorted offsets for all the encoded
// fields in the marshaled protobuf so that they can subsequently be iterated
// in sorted order.
func (u *sortedUnmarshalIter) calculateSortedOffsets() error {
	u.sortedOffsets = u.sortedOffsets[:0]
	u.sortedOffsetsIdx = 0

	u.skippedOffsets = u.skippedOffsets[:0]

	isSorted := true
	for !u.decodeBuf.eof() {
		// This offset is used for skipped fields that will be unmarshaled by the protobuf
		// library which will expect to decode the tag and wire type on its own.
		tagAndWireTypeStartOffset := u.decodeBuf.index
		tag, wireType, err := u.decodeBuf.decodeTagAndWireType()
		if err != nil {
			return err
		}
		// This offset is used for non-skipped fields so that the iterator can avoid
		// decoding the tag and wire type again.
		valueStartOffset := u.decodeBuf.index

		fd := u.schema.FindFieldByNumber(tag)
		if fd == nil {
			return fmt.Errorf("encountered unknown field with tag: %d", tag)
		}

		_, err = u.skip(tag, wireType)
		if err != nil {
			return err
		}

		length := u.decodeBuf.index - tagAndWireTypeStartOffset
		if u.shouldSkip(fd) {
			skippedOffset := skippedOffset{offset: tagAndWireTypeStartOffset, length: length}
			u.skippedOffsets = append(u.skippedOffsets, skippedOffset)
			u.skippedCount++
			continue
		}

		if isSorted && len(u.sortedOffsets) > 1 {
			// Check if the slice is sorted as its built to avoid resorting
			// unnecessarily at the end.
			lastOffset := u.sortedOffsets[len(u.sortedOffsets)-1].offset
			if tagAndWireTypeStartOffset < lastOffset {
				isSorted = false
			}
		}

		sortedOffset := sortedOffset{offset: valueStartOffset, fd: fd, wireType: wireType}
		u.sortedOffsets = append(u.sortedOffsets, sortedOffset)
	}

	u.decodeBuf.reset(u.decodeBuf.buf)

	if !isSorted {
		// Avoid resorting if possible.
		sort.Sort(u.sortedOffsets)
	}

	u.calculateSortedOffsetsComplete = true
	return nil
}

func (u *sortedUnmarshalIter) swapFn(i, j int) bool {
	return u.sortedOffsets[i].offset < u.sortedOffsets[j].offset
}

func (u *sortedUnmarshalIter) unmarshalField() error {
	// Reposition the decodeBuf to the correct offset so that it decodes the next
	// value according to correct sort order.
	sortedOffset := u.sortedOffsets[u.sortedOffsetsIdx]
	u.decodeBuf.index = sortedOffset.offset
	u.sortedOffsetsIdx++

	var err error
	u.last, err = u.unmarshalKnownField(sortedOffset.fd, sortedOffset.wireType)
	if err != nil {
		return err
	}

	return nil
}

func (u *sortedUnmarshalIter) shouldSkip(fd *desc.FieldDescriptor) bool {
	if fd.IsRepeated() || fd.IsMap() {
		// Map should always be repeated but include the guard just in case.
		return true
	}

	if fd.GetMessageType() != nil {
		// Skip nested messages.
		return true
	}

	return false
}

// skip will skip over the next value in the encoded stream (given that the tag and
// wiretype have already been decoded). Additionally, it can optionally re-encode
// the skipped <tag,wireType,value> tuple into the skippedBuf stream so that it can
// be handled later.
func (u *sortedUnmarshalIter) skip(tag int32, wireType int8) (int, error) {
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

func (u *sortedUnmarshalIter) unmarshalKnownField(fd *desc.FieldDescriptor, wireType int8) (unmarshalValue, error) {
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
		// of them until the call to Encode() completes and  they will get "copied" anyways
		// once they're written into the OStream.
		raw, err := u.decodeBuf.decodeRawBytes(false)
		if err != nil {
			return zeroValue, err
		}

		val := unmarshalValue{fd: fd}
		val.bytes = raw
		return val, nil

	case proto.WireStartGroup:
		return zeroValue, errGroupsAreNotSupported
	default:
		return zeroValue, proto.ErrInternalBadWireType
	}
}

func unmarshalSimpleField(fd *desc.FieldDescriptor, v uint64) (unmarshalValue, error) {
	val := unmarshalValue{fd: fd, v: v}
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

func (u *sortedUnmarshalIter) hasNext() bool {
	return u.sortedOffsetsIdx < len(u.sortedOffsets) && u.e == nil
}

func (u *sortedUnmarshalIter) err() error {
	return u.e
}

func (u *sortedUnmarshalIter) reset(schema *desc.MessageDescriptor, buf []byte) {
	if schema != u.schema {
		u.skippedMessage = dynamic.NewMessage(schema)
		u.skippedMessage.Reset()
	}

	u.schema = schema
	u.last = unmarshalValue{}
	u.e = nil
	u.skippedCount = 0
	u.calculateSortedOffsetsComplete = false
	u.sortedOffsets = u.sortedOffsets[:0]
	u.skippedOffsets = u.skippedOffsets[:0]
	u.sortedOffsetsIdx = 0
	// TODO: pools?
	u.decodeBuf.reset(buf)
}
