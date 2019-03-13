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
	"math"
	"reflect"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
)

const (
	byteFieldDictSize = 4
)

type customFieldType int

const (
	cSignedInt64 customFieldType = iota
	cSignedInt32
	cUnsignedInt64
	cUnsignedInt32
	cFloat64
	cFloat32
	cBytes
)

var (
	typeOfBytes = reflect.TypeOf(([]byte)(nil))

	mapProtoTypeToCustomFieldType = map[dpb.FieldDescriptorProto_Type]customFieldType{
		dpb.FieldDescriptorProto_TYPE_DOUBLE: cFloat64,
		dpb.FieldDescriptorProto_TYPE_FLOAT:  cFloat32,

		dpb.FieldDescriptorProto_TYPE_INT64:    cSignedInt64,
		dpb.FieldDescriptorProto_TYPE_SFIXED64: cSignedInt64,

		dpb.FieldDescriptorProto_TYPE_UINT64:  cUnsignedInt64,
		dpb.FieldDescriptorProto_TYPE_FIXED64: cUnsignedInt64,

		dpb.FieldDescriptorProto_TYPE_INT32:    cSignedInt32,
		dpb.FieldDescriptorProto_TYPE_SFIXED32: cSignedInt32,

		dpb.FieldDescriptorProto_TYPE_UINT32:  cUnsignedInt32,
		dpb.FieldDescriptorProto_TYPE_FIXED32: cUnsignedInt32,

		dpb.FieldDescriptorProto_TYPE_SINT32: cSignedInt32,
		dpb.FieldDescriptorProto_TYPE_SINT64: cSignedInt64,

		dpb.FieldDescriptorProto_TYPE_STRING: cBytes,
		dpb.FieldDescriptorProto_TYPE_BYTES:  cBytes,

		// dpb.FieldDescriptorProto_TYPE_ENUM:     struct{}{},

	}

	customIntEncodedFields = map[dpb.FieldDescriptorProto_Type]struct{}{
		// Signed.
		dpb.FieldDescriptorProto_TYPE_INT64:    struct{}{},
		dpb.FieldDescriptorProto_TYPE_INT32:    struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED32: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT64:   struct{}{},

		// Unsigned.
		dpb.FieldDescriptorProto_TYPE_UINT64:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_UINT32:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED32: struct{}{},
	}

	customEncodedFields = map[dpb.FieldDescriptorProto_Type]struct{}{
		dpb.FieldDescriptorProto_TYPE_DOUBLE:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_FLOAT:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_INT64:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_UINT64:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_INT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED32: struct{}{},
		dpb.FieldDescriptorProto_TYPE_STRING:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_BYTES:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_UINT32:  struct{}{},
		// dpb.FieldDescriptorProto_TYPE_ENUM:     struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED32: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT64:   struct{}{},
	}

	allowedProtoTypes = map[dpb.FieldDescriptorProto_Type]struct{}{
		dpb.FieldDescriptorProto_TYPE_DOUBLE:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_FLOAT:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_INT64:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_UINT64:  struct{}{},
		dpb.FieldDescriptorProto_TYPE_INT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_FIXED32: struct{}{},
		dpb.FieldDescriptorProto_TYPE_BOOL:    struct{}{},
		dpb.FieldDescriptorProto_TYPE_STRING:  struct{}{},
		// FieldDescriptorProto_TYPE_MESSAGE: struct{}{},
		dpb.FieldDescriptorProto_TYPE_BYTES:    struct{}{},
		dpb.FieldDescriptorProto_TYPE_UINT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_ENUM:     struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED32: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SFIXED64: struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT32:   struct{}{},
		dpb.FieldDescriptorProto_TYPE_SINT64:   struct{}{},
	}
)

type customFieldState struct {
	fieldNum  int
	fieldType customFieldType

	// Float state
	prevXOR       uint64
	prevFloatBits uint64

	// Bytes State
	bytesFieldDict         []uint64
	iteratorBytesFieldDict [][]byte

	intSigBitsTracker m3tsz.IntSigBitsTracker
}

// TODO(rartoul): SetTSZFields and numTSZFields are naive in that they don't handle
// repeated or nested messages / maps.
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

		s = append(s, customFieldState{
			fieldType: customFieldType,
			fieldNum:  int(field.GetNumber()),
		})
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
		if _, ok := customEncodedFields[fieldType]; ok {
			numCustomFields++
		}
	}

	return numCustomFields
}

func fieldsContains(fieldNum int32, fields []*desc.FieldDescriptor) bool {
	for _, field := range fields {
		if field.GetNumber() == fieldNum {
			return true
		}
	}
	return false
}

// numBitsRequiredToRepresentArrayIndex returns the number of bits that are required
// to represent all the possible indices of an array of size arrSize as a uint64. Its
// used to calculate the number of bits required to encode the index in the LRU for
// fields using streaming LRU dictionary compression like byte arrays and strings.
//
// 4   --> 2
// 8   --> 3
// 16  --> 4
// 32  --> 5
// 64  --> 6
// 128 --> 7
func numBitsRequiredToRepresentArrayIndex(arrSize int) int {
	return int(math.Log2(float64(arrSize)))
}
