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

	"github.com/golang/proto"
	"github.com/jhump/protoreflect/dynamic"
)

// Mostly copy-pasta of a non-exported helper method from the protoreflect
// library.
// https://github.com/jhump/protoreflect/blob/87f824e0b908132b2501fe5652f8ee75a2e8cf06/dynamic/equal.go#L60
func fieldsEqual(aval, bval interface{}) bool {
	arv := reflect.ValueOf(aval)
	brv := reflect.ValueOf(bval)

	if arv.Type() != brv.Type() {
		// it is possible that one is a dynamic message and one is not
		apm, ok := aval.(proto.Message)
		if !ok {
			return false
		}
		bpm, ok := bval.(proto.Message)
		if !ok {
			return false
		}
		if !dynamic.MessagesEqual(apm, bpm) {
			return false
		}
	} else {
		switch arv.Kind() {
		case reflect.Ptr:
			apm, ok := aval.(proto.Message)
			if !ok {
				// Don't know how to compare pointer values that aren't messages!
				// Maybe this should panic?
				return false
			}
			bpm := bval.(proto.Message) // we know it will succeed because we know a and b have same type
			if !dynamic.MessagesEqual(apm, bpm) {
				return false
			}
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
