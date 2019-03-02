package proto

import (
	"bytes"
	"reflect"
)

var (
	typeOfBytes = reflect.TypeOf(([]byte)(nil))
)

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
