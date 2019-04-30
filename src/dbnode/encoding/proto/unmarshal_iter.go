package proto

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

var (
	// ErrOverflow is returned when an integer is too large to be represented.
	ErrOverflow = errors.New("proto: integer overflow")

	errGroupsAreDeprecated = errors.New("use of groups in proto wire format are deprecated")

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
	schema      *desc.MessageDescriptor
	codedBuffer *codedBuffer
	last        unmarshalValue
	err         error
}

// TODO: Make smaller
type unmarshalValue struct {
	fd            *desc.FieldDescriptor
	fieldNum      int32
	boolVal       bool
	int32Val      int32
	int64Val      int64
	uint32Val     uint32
	uint64Val     uint64
	float32Val    float32
	float64Val    float64
	bytesVal      []byte
	stringVal     string
	messageVal    *dynamic.Message
	ifaceVal      interface{}
	ifaceSliceVal []interface{}
}

func newUnmarshalIter() *unmarshalIter {
	return &unmarshalIter{
		codedBuffer: newCodedBuffer(nil),
	}
}

func (u *unmarshalIter) next() bool {
	if !u.hasNext() {
		return false
	}

	err := u.unmarshalField(false)
	if err != nil {
		u.err = err
		return false
	}

	return true
}

func (u *unmarshalIter) current() unmarshalValue {
	return u.last
}

func (u *unmarshalIter) unmarshalField(isGroup bool) error {
	var isFirst = true
	for !u.codedBuffer.eof() && (isFirst || isGroup) {
		isFirst = false
		tag, wireType, err := u.codedBuffer.decodeTagAndWireType()
		if err != nil {
			return err
		}
		fmt.Println("tag: ", tag)

		if wireType == proto.WireEndGroup {
			panic("wire end")
			if isGroup {
				// Finished parsing group.
				return nil
			}
			return proto.ErrInternalBadWireType
		}

		fd := u.schema.FindFieldByNumber(tag)
		if fd == nil {
			return fmt.Errorf("encountered unknown field with tag: %d", tag)
		}

		u.last, err = u.unmarshalKnownField(fd, wireType)
		if err != nil {
			return err
		}
	}

	// TODO: Maybe delete this code path
	if isGroup {
		return io.ErrUnexpectedEOF
	}

	fmt.Println("eof?", u.codedBuffer.eof())
	return nil
}

func (u *unmarshalIter) unmarshalKnownField(fd *desc.FieldDescriptor, wireType int8) (unmarshalValue, error) {
	switch wireType {
	case proto.WireFixed32:
		num, err := u.codedBuffer.decodeFixed32()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)
	case proto.WireFixed64:
		num, err := u.codedBuffer.decodeFixed64()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)
	case proto.WireVarint:
		num, err := u.codedBuffer.decodeVarint()
		if err != nil {
			return zeroValue, err
		}
		return unmarshalSimpleField(fd, num)

	case proto.WireBytes:
		if fd.GetType() == dpb.FieldDescriptorProto_TYPE_BYTES ||
			fd.GetType() == dpb.FieldDescriptorProto_TYPE_STRING {
			raw, err := u.codedBuffer.decodeRawBytes(true) // defensive copy
			if err != nil {
				return zeroValue, err
			}

			val := unmarshalValue{fd: fd}
			if fd.GetType() == dpb.FieldDescriptorProto_TYPE_BYTES {
				val.bytesVal = raw
				return val, nil
			}

			val.stringVal = string(raw)
			return val, nil
		}

		raw, err := u.codedBuffer.decodeRawBytes(false)
		if err != nil {
			return zeroValue, err
		}

		return unmarshalLengthDelimitedField(fd, raw)

	case proto.WireStartGroup:
		fmt.Println("group!???")
		return zeroValue, errGroupsAreDeprecated
	default:
		return zeroValue, proto.ErrInternalBadWireType
	}
}

func unmarshalSimpleField(fd *desc.FieldDescriptor, v uint64) (unmarshalValue, error) {
	val := unmarshalValue{fd: fd}
	switch fd.GetType() {
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		val.boolVal = v != 0
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_UINT32,
		dpb.FieldDescriptorProto_TYPE_FIXED32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.uint32Val = uint32(v)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_ENUM:
		s := int64(v)
		if s > math.MaxInt32 || s < math.MinInt32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.int32Val = int32(s)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SFIXED32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.int32Val = int32(v)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SINT32:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.int32Val = decodeZigZag32(v)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_UINT64,
		dpb.FieldDescriptorProto_TYPE_FIXED64:
		val.uint64Val = v
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_SFIXED64:
		val.int64Val = int64(v)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_SINT64:
		val.int64Val = decodeZigZag64(v)
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		if v > math.MaxUint32 {
			return zeroValue, dynamic.NumericOverflowError
		}
		val.float32Val = math.Float32frombits(uint32(v))
		return val, nil

	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		val.float64Val = math.Float64frombits(v)
		return val, nil

	default:
		// bytes, string, message, and group cannot be represented as a simple numeric value
		return zeroValue, fmt.Errorf("bad input; field %s requires length-delimited wire type", fd.GetFullyQualifiedName())
	}
}

func unmarshalLengthDelimitedField(fd *desc.FieldDescriptor, bytes []byte) (unmarshalValue, error) {
	val := unmarshalValue{fd: fd}
	switch {
	case fd.GetType() == dpb.FieldDescriptorProto_TYPE_BYTES:
		val.bytesVal = bytes
		return val, nil

	case fd.GetType() == dpb.FieldDescriptorProto_TYPE_STRING:
		val.stringVal = string(bytes)
		return val, nil

	case fd.GetType() == dpb.FieldDescriptorProto_TYPE_MESSAGE ||
		fd.GetType() == dpb.FieldDescriptorProto_TYPE_GROUP:
		fmt.Println("type message or group, map?", fd.IsMap())
		fmt.Println("type:", fd.GetType())
		fmt.Println("message type", fd.GetMessageType())
		msg := dynamic.NewMessage(fd.GetMessageType())
		err := proto.Unmarshal(bytes, msg)
		if err != nil {
			return zeroValue, err
		}

		val.messageVal = msg
		return val, nil

	default:
		fmt.Println("REPEATED")
		// even if the field is not repeated or not packed, we still parse it as such for
		// backwards compatibility (e.g. message we are de-serializing could have been both
		// repeated and packed at the time of serialization)
		packedBuf := newCodedBuffer(bytes)
		var slice []interface{}
		var ifaceVal interface{}
		for !packedBuf.eof() {
			var v uint64
			var err error
			if varintTypes[fd.GetType()] {
				v, err = packedBuf.decodeVarint()
			} else if fixed32Types[fd.GetType()] {
				v, err = packedBuf.decodeFixed32()
			} else if fixed64Types[fd.GetType()] {
				v, err = packedBuf.decodeFixed64()
			} else {
				return zeroValue, fmt.Errorf("bad input; cannot parse length-delimited wire type for field %s", fd.GetFullyQualifiedName())
			}
			if err != nil {
				return zeroValue, err
			}
			ifaceVal, err = unmarshalSimpleField(fd, v)
			if err != nil {
				return zeroValue, err
			}
			if fd.IsRepeated() {
				slice = append(slice, ifaceVal)
			}
		}

		if fd.IsRepeated() {
			val.ifaceSliceVal = slice
			return val, nil
		}

		// TODO: Return error here? Not sure when this can happen
		// if not a repeated field, last value wins
		val.ifaceVal = val
		return val, nil
	}
}

func (u *unmarshalIter) skip(wireType int8) {

}

func (u *unmarshalIter) hasNext() bool {
	return !u.codedBuffer.eof() && u.err == nil
}

func (u *unmarshalIter) reset(schema *desc.MessageDescriptor, buf []byte) {
	u.schema = schema
	u.last = unmarshalValue{}
	u.err = nil
	u.codedBuffer.reset(buf)
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

func (cb *codedBuffer) skip(count int) bool {
	newIndex := cb.index + count
	if newIndex > len(cb.buf) {
		return false
	}
	cb.index = newIndex
	return true
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

func encodeZigZag64(v int64) uint64 {
	return (uint64(v) << 1) ^ uint64(v>>63)
}

func encodeZigZag32(v int32) uint64 {
	return uint64((uint32(v) << 1) ^ uint32((v >> 31)))
}

// EncodeRawBytes writes a count-delimited byte buffer to the Buffer.
// This is the format used for the bytes protocol buffer
// type and for embedded messages.
func (cb *codedBuffer) encodeRawBytes(b []byte) error {
	cb.encodeVarint(uint64(len(b)))
	cb.buf = append(cb.buf, b...)
	return nil
}

func (cb *codedBuffer) encodeMessage(pm proto.Message) error {
	bytes, err := proto.Marshal(pm)
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return nil
	}

	if err := cb.encodeVarint(uint64(len(bytes))); err != nil {
		return err
	}
	cb.buf = append(cb.buf, bytes...)
	return nil
}
