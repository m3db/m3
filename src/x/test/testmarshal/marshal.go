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

// Package testmarshal provides some assertions around marshalling/unmarshalling
// (serialization/deserialization) behavior for types. It is intended to reduce
// boilerplate in tests of the form:
//
// func TestMyTypeUnmarshals(t *testing.T) {
// 		type MyType struct{}
// 		var mt MyType
// 		require.NoError(t, json.Unmarshal([]byte("{}"), &mt))
// 		assert.Equal(t, MyType{}, mt)
// }
//
// with assertion calls:
// func TestMyTypeUnmarshals(t *testing.T) {
// 		type MyType struct{}
//      testmarshal.AssertUnmarshals(t, testmarshal.JSONMarshaler, MyType{}, []byte("{}"))
// }
package testmarshal

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

// Marshaler represents a serialization protocol, e.g. JSON or YAML
type Marshaler interface {
	// Marshal converts a Go type into bytes
	Marshal(interface{}) ([]byte, error)

	// Unmarshal converts bytes into a Go type.
	Unmarshal([]byte, interface{}) error

	// ID identifies the protocol, mostly for use in test naming.
	ID() string
}

var (
	// JSONMarshaler uses the encoding/json package to marshal types
	JSONMarshaler Marshaler = simpleMarshaler{
		id:        "json",
		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}

	// YAMLMarshaler uses the gopkg.in/yaml.v2 package to marshal types
	YAMLMarshaler Marshaler = simpleMarshaler{
		id:        "yaml",
		marshal:   yaml.Marshal,
		unmarshal: yaml.Unmarshal,
	}

	// TextMarshaler marshals types which implement both encoding.TextMarshaler
	// and encoding.TextUnmarshaler
	TextMarshaler Marshaler = simpleMarshaler{
		id: "text",
		marshal: func(i interface{}) ([]byte, error) {
			switch m := i.(type) {
			case encoding.TextMarshaler:
				return m.MarshalText()
			default:
				return nil, fmt.Errorf("not an encoding.TextMarshaler")
			}
		},
		unmarshal: func(bytes []byte, i interface{}) error {
			switch m := i.(type) {
			case encoding.TextUnmarshaler:
				return m.UnmarshalText(bytes)
			default:
				return fmt.Errorf("not an encoding.TextUnmarshaler")
			}
		},
	}
)

type simpleMarshaler struct {
	marshal   func(interface{}) ([]byte, error)
	unmarshal func([]byte, interface{}) error
	id        string
}

func (sm simpleMarshaler) Marshal(v interface{}) ([]byte, error) {
	return sm.marshal(v)
}

func (sm simpleMarshaler) Unmarshal(d []byte, v interface{}) error {
	return sm.unmarshal(d, v)
}

func (sm simpleMarshaler) ID() string {
	return sm.id
}

// AssertMarshalingRoundtrips checks that the marshaller "roundtrips" example
// i.e.:
// marshaler.Unmarshal(marshaler.Marshal(example)) == example.
//
// It is intended to replace tests of the form:
//
// func TestMyTypeRoundtrips(t *testing.T) {
// 	type MyType struct{}
// 	mt := MyType{}
// 	d, err := json.Marshal(mt)
// 	require.NoError(t, err)
//
// 	var revived MyType
// 	require.NoError(t, json.Unmarshal(d, &revived))
// 	assert.Equal(t, mt, revived)
// }
//
// with:

// func TestMyTypeRoundtrips(t *testing.T) {
// 	type MyType struct{}
// 	testmarshal.AssertMarshalingRoundtrips(t, testmarshal.JSONMarshaler, MyType{})
// }
func AssertMarshalingRoundtrips(t *testing.T, marshaller Marshaler, example interface{}) bool {
	d, err := marshaller.Marshal(example)
	if !assert.NoError(t, err) {
		return false
	}
	reconstituted, err := unmarshalIntoNewValueOfType(marshaller, example, d)
	if !assert.NoError(t, err) {
		return false
	}

	return assert.Equal(t, example, reconstituted)
}

// AssertUnmarshals checks that the given data successfully unmarshals into a
// value which assert.Equal's expected.
// It is intended to replace tests of the form:
//
// func TestMyTypeUnmarshals(t *testing.T) {
// 	type MyType struct{}
// 	var mt MyType
// 	require.NoError(t, json.Unmarshal([]byte("{}"), &mt))
// 	assert.Equal(t, MyType{}, mt)
// }
//
// with:

// func TestMyTypeUnmarshals(t *testing.T) {
//      type MyType struct{}
//      testmarshal.AssertUnmarshals(t, testmarshal.JSONMarshaler, MyType{}, []byte("{}"))
// }

func AssertUnmarshals(t *testing.T, marshaller Marshaler, expected interface{}, data []byte) bool {
	unmarshalled, err := unmarshalIntoNewValueOfType(marshaller, expected, data)
	if !assert.NoError(t, err) {
		return false
	}

	return assert.Equal(t, expected, unmarshalled)
}

// AssertMarshals checks that the given value marshals into data equal
// to expectedData.
//  It is intended to replace tests of the form:
//
// func TestMyTypeMarshals(t *testing.T) {
// 	type MyType struct{}
//    mt := MyType{}
//    d, err := json.Marshal(mt)
//    require.NoError(t, err)
//    assert.Equal(t, d, []byte("{}"))
// }
//
// with:
//
// func TestMyTypeUnmarshals(t *testing.T) {
// 	 type MyType struct{}
// 	 testmarshal.AssertMarshals(t, testmarshal.JSONMarshaler, MyType{}, []byte("{}"))
// }
func AssertMarshals(t *testing.T, marshaller Marshaler, toMarshal interface{}, expectedData []byte) bool {
	marshalled, err := marshaller.Marshal(toMarshal)
	if !assert.NoError(t, err) {
		return false
	}

	return assert.Equal(t, string(expectedData), string(marshalled))
}

// unmarshalIntoNewValueOfType is a helper to unmarshal a new instance of the same type as value
// from data.
func unmarshalIntoNewValueOfType(marshaller Marshaler, value interface{}, data []byte) (interface{}, error) {
	ptrToUnmarshalTarget := reflect.New(reflect.ValueOf(value).Type())
	unmarshalTarget := ptrToUnmarshalTarget.Elem()
	if err := marshaller.Unmarshal(data, ptrToUnmarshalTarget.Interface()); err != nil {
		return nil, err
	}
	return unmarshalTarget.Interface(), nil
}

// Require wraps an Assert call and turns it into a require.* call (fails if
// the assert fails).
func Require(t *testing.T, b bool) {
	if !b {
		t.FailNow()
	}
}

// TestMarshalersRoundtrip is a helper which runs a test for each provided marshaller
// on each example in examples (a slice of any type). The test checks that the
// marshaler "roundtrips" for each example, i.e.:
// marshaler.Unmarshal(marshaler.Marshal(example)) == example.
func TestMarshalersRoundtrip(t *testing.T, examples interface{}, marshallers []Marshaler) {
	for _, m := range marshallers {
		t.Run(m.ID(), func(t *testing.T) {
			v := reflect.ValueOf(examples)
			if v.Type().Kind() != reflect.Slice {
				t.Fatalf("examples must be a slice; got %+v", examples)
			}

			// taken from https://stackoverflow.com/questions/14025833/range-over-interface-which-stores-a-slice
			for i := 0; i < v.Len(); i++ {
				example := v.Index(i).Interface()
				Require(t, AssertMarshalingRoundtrips(t, m, example))
			}
		})

	}
}
