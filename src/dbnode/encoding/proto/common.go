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
	"reflect"

	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

type Schema *desc.MessageDescriptor

const (
	// ~1GiB is an intentionally a very large number to avoid users ever running into any
	// limitations, but we want some theoretical maximum so that in the case of data / memory
	// corruption the iterator can avoid panicing due to trying to allocate a massive byte slice
	// (MAX_UINT64 for example) and return a reasonable error message instead.
	maxMarshaledProtoMessageSize = 2 << 29

	// maxCustomFieldNum is included for the same rationale as maxMarshaledProtoMessageSize.
	maxCustomFieldNum = 10000
)

type customFieldType int

const (
	// All the protobuf field types that we can perform custom encoding /
	// compression on will get mapped to one of these types. This prevents
	// us from having to reference the protobuf type all over the encoder
	// and iterators and also simplifies the logic because the protobuf
	// format has several instances of multiple types that we will treat the
	// same. For example, in our encoding scheme the proto types:
	// int32, sfixed32, and enums are all are treated as int32s and there
	// is no reasonm to distinguish between them for the purposes of encoding
	// and decoding.
	cNotCustomEncoded customFieldType = iota
	cSignedInt64
	cSignedInt32
	cUnsignedInt64
	cUnsignedInt32
	cFloat64
	cFloat32
	cBytes

	numCustomTypes = 8
)

// -1 because iota's are zero-indexed so the highest value will be the number of
// custom types - 1.
var numBitsToEncodeCustomType = numBitsRequiredForNumUpToN(numCustomTypes - 1)

const (
	// Single bit op codes that get encoded into the compressed stream and
	// inform the iterator / decoder how it should interpret subsequent
	// bits.
	opCodeNoMoreData = 0
	opCodeMoreData   = 1

	opCodeNoChange = 0
	opCodeChange   = 1

	opCodeInterpretSubsequentBitsAsLRUIndex          = 0
	opCodeInterpretSubsequentBitsAsBytesLengthVarInt = 1

	opCodeNoFieldsSetToDefaultProtoMarshal = 0
	opCodeFieldsSetToDefaultProtoMarshal   = 1

	opCodeIntDeltaPositive = 0
	opCodeIntDeltaNegative = 1

	opCodeBitsetValueIsNotSet = 0
	opCodeBitsetValueIsSet    = 1
)

var (
	typeOfBytes = reflect.TypeOf(([]byte)(nil))

	// Maps protobuf types to our custom type as described above.
	mapProtoTypeToCustomFieldType = map[dpb.FieldDescriptorProto_Type]customFieldType{
		dpb.FieldDescriptorProto_TYPE_DOUBLE: cFloat64,
		dpb.FieldDescriptorProto_TYPE_FLOAT:  cFloat32,

		dpb.FieldDescriptorProto_TYPE_INT64:    cSignedInt64,
		dpb.FieldDescriptorProto_TYPE_SFIXED64: cSignedInt64,

		dpb.FieldDescriptorProto_TYPE_UINT64:  cUnsignedInt64,
		dpb.FieldDescriptorProto_TYPE_FIXED64: cUnsignedInt64,

		dpb.FieldDescriptorProto_TYPE_INT32:    cSignedInt32,
		dpb.FieldDescriptorProto_TYPE_SFIXED32: cSignedInt32,
		// Signed because thats how Proto encodes it (can technically have negative
		// enum values but its not recommended for compression reasons).
		dpb.FieldDescriptorProto_TYPE_ENUM: cSignedInt32,

		dpb.FieldDescriptorProto_TYPE_UINT32:  cUnsignedInt32,
		dpb.FieldDescriptorProto_TYPE_FIXED32: cUnsignedInt32,

		dpb.FieldDescriptorProto_TYPE_SINT32: cSignedInt32,
		dpb.FieldDescriptorProto_TYPE_SINT64: cSignedInt64,

		dpb.FieldDescriptorProto_TYPE_STRING: cBytes,
		dpb.FieldDescriptorProto_TYPE_BYTES:  cBytes,
	}
)

// customFieldState is used to track any required state for encoding / decoding a single
// field in the encoder / iterator respectively.
type customFieldState struct {
	// TODO(rartoul): This could be made more efficient by separating out fields required
	// for encoding and those required for enumeration, as well as trying to reuse some
	// of the fields for multiple types to save memory, but its ok for now.
	fieldNum  int
	fieldType customFieldType

	// Float state.
	prevXOR       uint64
	prevFloatBits uint64

	// Bytes State.
	bytesFieldDict         []uint64
	iteratorBytesFieldDict [][]byte

	// Int state.
	intSigBitsTracker m3tsz.IntSigBitsTracker
}

func newCustomFieldState(fieldNum int, fieldType customFieldType) customFieldState {
	return customFieldState{fieldNum: fieldNum, fieldType: fieldType}
}

// TODO(rartoul): Improve this function to be less naive and actually explore nested messages
// for fields that we can use our custom compression on: https://github.com/m3db/m3/issues/1471
func customFields(s []customFieldState, schema *desc.MessageDescriptor) []customFieldState {
	numCustomFields := numCustomFields(schema)
	if cap(s) >= numCustomFields {
		s = s[:0]
	} else {
		s = make([]customFieldState, 0, numCustomFields)
	}

	fields := schema.GetFields()
	for _, field := range fields {
		fieldType := field.GetType()
		customFieldType, ok := mapProtoTypeToCustomFieldType[fieldType]
		if !ok {
			continue
		}

		fieldState := newCustomFieldState(int(field.GetNumber()), customFieldType)
		s = append(s, fieldState)
	}

	return s
}

func isCustomFloatEncodedField(t customFieldType) bool {
	return t == cFloat64 || t == cFloat32
}

func isCustomIntEncodedField(t customFieldType) bool {
	return t == cSignedInt64 ||
		t == cUnsignedInt64 ||
		t == cSignedInt32 ||
		t == cUnsignedInt32
}

func isUnsignedInt(t customFieldType) bool {
	return t == cUnsignedInt64 || t == cUnsignedInt32
}

func numCustomFields(schema *desc.MessageDescriptor) int {
	var (
		fields          = schema.GetFields()
		numCustomFields = 0
	)

	for _, field := range fields {
		fieldType := field.GetType()
		if _, ok := mapProtoTypeToCustomFieldType[fieldType]; ok {
			numCustomFields++
		}
	}

	return numCustomFields
}

func resetCustomFields(fields []customFieldState, schema *desc.MessageDescriptor) []customFieldState {
	if cap(fields) <= maxTSZFieldsCapacityRetain {
		return customFields(fields, schema)
	}
	return customFields(nil, schema)
}

func fieldsContains(fieldNum int32, fields []*desc.FieldDescriptor) bool {
	for _, field := range fields {
		if field.GetNumber() == fieldNum {
			return true
		}
	}
	return false
}

// numBitsRequiredForNumUpToN returns the number of bits that are required
// to represent all the possible numbers between 0 and n as a uint64.
//
// 4   --> 2
// 8   --> 3
// 16  --> 4
// 32  --> 5
// 64  --> 6
// 128 --> 7
func numBitsRequiredForNumUpToN(n int) int {
	count := 0
	for n > 0 {
		count++
		n = n >> 1
	}
	return count
}
