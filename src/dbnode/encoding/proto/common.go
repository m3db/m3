package proto

import (
	"reflect"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
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

// TODO(rartoul): SetTSZFields and numTSZFields are naive in that they don't handle
// repeated or nested messages / maps.
// TODO(rartoul): Should handle integers as TSZ as well, can just do XOR on the regular
// bits after converting to uint64. Just need to check type on encode/iterate to determine
// how to interpret bits.
func tszFields(s []tszFieldState, schema *desc.MessageDescriptor) []tszFieldState {
	numTSZFields := numTSZFields(schema)
	if cap(s) >= numTSZFields {
		s = s[:0]
	} else {
		s = make([]tszFieldState, 0, numTSZFields)
	}

	fields := schema.GetFields()
	for _, field := range fields {
		fieldType := field.GetType()
		if fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE ||
			fieldType == dpb.FieldDescriptorProto_TYPE_FLOAT {
			s = append(s, tszFieldState{
				fieldNum: int(field.GetNumber()),
			})
		}
	}

	return s
}

func numTSZFields(schema *desc.MessageDescriptor) int {
	var (
		fields       = schema.GetFields()
		numTSZFields = 0
	)

	for _, field := range fields {
		fieldType := field.GetType()
		if fieldType == dpb.FieldDescriptorProto_TYPE_DOUBLE ||
			fieldType == dpb.FieldDescriptorProto_TYPE_FLOAT {
			numTSZFields++
		}
	}

	return numTSZFields
}

func fieldsContains(fieldNum int32, fields []*desc.FieldDescriptor) bool {
	for _, field := range fields {
		if field.GetNumber() == fieldNum {
			return true
		}
	}
	return false
}
