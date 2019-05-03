package proto

import (
	"errors"
	"fmt"
	"io"
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

	varintTypes  = map[dpb.FieldDescriptorProto_Type]bool{}
	fixed32Types = map[dpb.FieldDescriptorProto_Type]bool{}
	fixed64Types = map[dpb.FieldDescriptorProto_Type]bool{}
)

func init() {
	varintTypes[dpb.FieldDescriptorProto_TYPE_BOOL] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_INT32] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_INT64] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_UINT32] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_UINT64] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_SINT32] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_SINT64] = true
	varintTypes[dpb.FieldDescriptorProto_TYPE_ENUM] = true

	fixed32Types[dpb.FieldDescriptorProto_TYPE_FIXED32] = true
	fixed32Types[dpb.FieldDescriptorProto_TYPE_SFIXED32] = true
	fixed32Types[dpb.FieldDescriptorProto_TYPE_FLOAT] = true

	fixed64Types[dpb.FieldDescriptorProto_TYPE_FIXED64] = true
	fixed64Types[dpb.FieldDescriptorProto_TYPE_SFIXED64] = true
	fixed64Types[dpb.FieldDescriptorProto_TYPE_DOUBLE] = true
}

type unmarshalIter struct {
	schema *desc.MessageDescriptor

	decodeBuf *codedBuffer

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

func newUnmarshalIter() *unmarshalIter {
	return &unmarshalIter{
		decodeBuf: newCodedBuffer(nil),
	}
}

func (u *unmarshalIter) next() bool {
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

func (u *unmarshalIter) current() unmarshalValue {
	return u.last
}

func (u *unmarshalIter) numSkipped() int {
	return u.skippedCount
}

func (u *unmarshalIter) skipped() (*dynamic.Message, error) {
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
func (u *unmarshalIter) calculateSortedOffsets() error {
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
			// it at the end if there is no need.
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

func (u *unmarshalIter) swapFn(i, j int) bool {
	return u.sortedOffsets[i].offset < u.sortedOffsets[j].offset
}

func (u *unmarshalIter) unmarshalField() error {
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

func (u *unmarshalIter) shouldSkip(fd *desc.FieldDescriptor) bool {
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
func (u *unmarshalIter) skip(tag int32, wireType int8) (int, error) {
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

func (u *unmarshalIter) unmarshalKnownField(fd *desc.FieldDescriptor, wireType int8) (unmarshalValue, error) {
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

func (u *unmarshalIter) hasNext() bool {
	return u.sortedOffsetsIdx < len(u.sortedOffsets) && u.e == nil
}

func (u *unmarshalIter) err() error {
	return u.e
}

func (u *unmarshalIter) reset(schema *desc.MessageDescriptor, buf []byte) {
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

// A reader/writer type that assists with encoding and decoding protobuf's binary representation.
// This code is largely a fork of proto.Buffer, which cannot be used because it has no exported
// field or method that provides access to its underlying reader index.
type codedBuffer struct {
	buf   []byte
	index int
}

func newCodedBuffer(buf []byte) *codedBuffer {
	return &codedBuffer{buf: buf}
}

func (cb *codedBuffer) reset(b []byte) {
	cb.buf = b
	cb.index = 0
}

func (cb *codedBuffer) eof() bool {
	return cb.index >= len(cb.buf)
}

func (cb *codedBuffer) skip(count int) (int, bool) {
	newIndex := cb.index + count
	if newIndex > len(cb.buf) {
		return 0, false
	}
	cb.index = newIndex
	return 0, true
}

func (cb *codedBuffer) decodeVarintSlow() (x uint64, err error) {
	i := cb.index
	l := len(cb.buf)

	for shift := uint(0); shift < 64; shift += 7 {
		if i >= l {
			err = io.ErrUnexpectedEOF
			return
		}
		b := cb.buf[i]
		i++
		x |= (uint64(b) & 0x7F) << shift
		if b < 0x80 {
			cb.index = i
			return
		}
	}

	// The number is too large to represent in a 64-bit value.
	err = ErrOverflow
	return
}

// DecodeVarint reads a varint-encoded integer from the Buffer.
// This is the format for the
// int32, int64, uint32, uint64, bool, and enum
// protocol buffer types.
func (cb *codedBuffer) decodeVarint() (uint64, error) {
	i := cb.index
	buf := cb.buf

	if i >= len(buf) {
		return 0, io.ErrUnexpectedEOF
	} else if buf[i] < 0x80 {
		cb.index++
		return uint64(buf[i]), nil
	} else if len(buf)-i < 10 {
		return cb.decodeVarintSlow()
	}

	var b uint64
	// we already checked the first byte
	x := uint64(buf[i]) - 0x80
	i++

	b = uint64(buf[i])
	i++
	x += b << 7
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 7

	b = uint64(buf[i])
	i++
	x += b << 14
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 14

	b = uint64(buf[i])
	i++
	x += b << 21
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 21

	b = uint64(buf[i])
	i++
	x += b << 28
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 28

	b = uint64(buf[i])
	i++
	x += b << 35
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 35

	b = uint64(buf[i])
	i++
	x += b << 42
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 42

	b = uint64(buf[i])
	i++
	x += b << 49
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 49

	b = uint64(buf[i])
	i++
	x += b << 56
	if b&0x80 == 0 {
		goto done
	}
	x -= 0x80 << 56

	b = uint64(buf[i])
	i++
	x += b << 63
	if b&0x80 == 0 {
		goto done
	}
	// x -= 0x80 << 63 // Always zero.

	return 0, ErrOverflow

done:
	cb.index = i
	return x, nil
}

func (cb *codedBuffer) decodeTagAndWireType() (tag int32, wireType int8, err error) {
	var v uint64
	v, err = cb.decodeVarint()
	if err != nil {
		return
	}
	// low 7 bits is wire type
	wireType = int8(v & 7)
	// rest is int32 tag number
	v = v >> 3
	if v > math.MaxInt32 {
		err = fmt.Errorf("tag number out of range: %d", v)
		return
	}
	tag = int32(v)
	return
}

// DecodeFixed64 reads a 64-bit integer from the Buffer.
// This is the format for the
// fixed64, sfixed64, and double protocol buffer types.
func (cb *codedBuffer) decodeFixed64() (x uint64, err error) {
	// x, err already 0
	i := cb.index + 8
	if i < 0 || i > len(cb.buf) {
		err = io.ErrUnexpectedEOF
		return
	}
	cb.index = i

	x = uint64(cb.buf[i-8])
	x |= uint64(cb.buf[i-7]) << 8
	x |= uint64(cb.buf[i-6]) << 16
	x |= uint64(cb.buf[i-5]) << 24
	x |= uint64(cb.buf[i-4]) << 32
	x |= uint64(cb.buf[i-3]) << 40
	x |= uint64(cb.buf[i-2]) << 48
	x |= uint64(cb.buf[i-1]) << 56
	return
}

// DecodeFixed32 reads a 32-bit integer from the Buffer.
// This is the format for the
// fixed32, sfixed32, and float protocol buffer types.
func (cb *codedBuffer) decodeFixed32() (x uint64, err error) {
	// x, err already 0
	i := cb.index + 4
	if i < 0 || i > len(cb.buf) {
		err = io.ErrUnexpectedEOF
		return
	}
	cb.index = i

	x = uint64(cb.buf[i-4])
	x |= uint64(cb.buf[i-3]) << 8
	x |= uint64(cb.buf[i-2]) << 16
	x |= uint64(cb.buf[i-1]) << 24
	return
}

func decodeZigZag32(v uint64) int32 {
	return int32((uint32(v) >> 1) ^ uint32((int32(v&1)<<31)>>31))
}

func decodeZigZag64(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

// These are not ValueDecoders: they produce an array of bytes or a string.
// bytes, embedded messages

// DecodeRawBytes reads a count-delimited byte buffer from the Buffer.
// This is the format used for the bytes protocol buffer
// type and for embedded messages.
func (cb *codedBuffer) decodeRawBytes(alloc bool) (buf []byte, err error) {
	n, err := cb.decodeVarint()
	if err != nil {
		return nil, err
	}

	nb := int(n)
	if nb < 0 {
		return nil, fmt.Errorf("proto: bad byte length %d", nb)
	}
	end := cb.index + nb
	if end < cb.index || end > len(cb.buf) {
		return nil, io.ErrUnexpectedEOF
	}

	if !alloc {
		buf = cb.buf[cb.index:end]
		cb.index += nb
		return
	}

	buf = make([]byte, nb)
	copy(buf, cb.buf[cb.index:])
	cb.index += nb
	return
}

// EncodeVarint writes a varint-encoded integer to the Buffer.
// This is the format for the
// int32, int64, uint32, uint64, bool, and enum
// protocol buffer types.
func (cb *codedBuffer) encodeVarint(x uint64) error {
	for x >= 1<<7 {
		cb.buf = append(cb.buf, uint8(x&0x7f|0x80))
		x >>= 7
	}
	cb.buf = append(cb.buf, uint8(x))
	return nil
}

func (cb *codedBuffer) encodeTagAndWireType(tag int32, wireType int8) error {
	v := uint64((int64(tag) << 3) | int64(wireType))
	return cb.encodeVarint(v)
}

// EncodeFixed64 writes a 64-bit integer to the Buffer.
// This is the format for the
// fixed64, sfixed64, and double protocol buffer types.
func (cb *codedBuffer) encodeFixed64(x uint64) error {
	cb.buf = append(cb.buf,
		uint8(x),
		uint8(x>>8),
		uint8(x>>16),
		uint8(x>>24),
		uint8(x>>32),
		uint8(x>>40),
		uint8(x>>48),
		uint8(x>>56))
	return nil
}

// EncodeFixed32 writes a 32-bit integer to the Buffer.
// This is the format for the
// fixed32, sfixed32, and float protocol buffer types.
func (cb *codedBuffer) encodeFixed32(x uint64) error {
	cb.buf = append(cb.buf,
		uint8(x),
		uint8(x>>8),
		uint8(x>>16),
		uint8(x>>24))
	return nil
}

// EncodeRawBytes writes a count-delimited byte buffer to the Buffer.
// This is the format used for the bytes protocol buffer
// type and for embedded messages.
func (cb *codedBuffer) encodeRawBytes(b []byte) error {
	cb.encodeVarint(uint64(len(b)))
	cb.buf = append(cb.buf, b...)
	return nil
}
