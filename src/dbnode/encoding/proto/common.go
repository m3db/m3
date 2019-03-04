package proto

import (
	"bytes"
	"reflect"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

var (
	typeOfBytes = reflect.TypeOf(([]byte)(nil))
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

func fieldsEqual(aval, bval interface{}) bool {
	arv := reflect.ValueOf(aval)
	brv := reflect.ValueOf(bval)

	switch arv.Kind() {
	case reflect.Ptr:
		panic("unreachable")
	case reflect.Map:
		if !mapsEqual(arv, brv) {
			return false
		}

	case reflect.Slice:
		if arv.Type() == typeOfBytes {
			if !bytes.Equal(aval.([]byte), bval.([]byte)) {
				return false
			}
		} else {
			if !slicesEqual(arv, brv) {
				return false
			}
		}

	default:
		if aval != bval {
			return false
		}
	}

	return true
}

func mapsEqual(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for _, k := range a.MapKeys() {
		av := a.MapIndex(k)
		bv := b.MapIndex(k)
		if !bv.IsValid() {
			return false
		}
		if !fieldsEqual(av.Interface(), bv.Interface()) {
			return false
		}
	}
	return true
}

func slicesEqual(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		ai := a.Index(i)
		bi := b.Index(i)
		if !fieldsEqual(ai.Interface(), bi.Interface()) {
			return false
		}
	}
	return true
}
