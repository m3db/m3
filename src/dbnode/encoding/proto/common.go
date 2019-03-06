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
	"reflect"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/snappy"
	"github.com/jhump/protoreflect/desc"
)

var (
	typeOfBytes = reflect.TypeOf(([]byte)(nil))

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
	fieldType dpb.FieldDescriptorProto_Type

	// Float state
	prevXOR       uint64
	prevFloatBits uint64

	// Bytes State
	prevBytes   []byte
	bytesWriter *snappy.Writer
	bytesBuf    *bytes.Buffer
}

// TODO(rartoul): SetTSZFields and numTSZFields are naive in that they don't handle
// repeated or nested messages / maps.
// TODO(rartoul): Should handle integers as TSZ as well, can just do XOR on the regular
// bits after converting to uint64. Just need to check type on encode/iterate to determine
// how to interpret bits.
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
		if fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE ||
			fieldType == dpb.FieldDescriptorProto_TYPE_FLOAT {
			s = append(s, customFieldState{
				fieldType: dpb.FieldDescriptorProto_TYPE_DOUBLE,
				fieldNum:  int(field.GetNumber()),
			})
		}

		if fieldType == dpb.FieldDescriptorProto_TYPE_BYTES ||
			fieldType == dpb.FieldDescriptorProto_TYPE_STRING {
			s = append(s, customFieldState{
				fieldType: dpb.FieldDescriptorProto_TYPE_BYTES,
				fieldNum:  int(field.GetNumber()),
			})
		}
	}

	return s
}

func numCustomFields(schema *desc.MessageDescriptor) int {
	var (
		fields          = schema.GetFields()
		numCustomFields = 0
	)

	for _, field := range fields {
		fieldType := field.GetType()
		// TODO: Put in a map or make a helper function
		if fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE ||
			fieldType == dpb.FieldDescriptorProto_TYPE_FLOAT ||
			fieldType == dpb.FieldDescriptorProto_TYPE_BYTES ||
			fieldType == dpb.FieldDescriptorProto_TYPE_STRING {
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
