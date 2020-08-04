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

package testmarshal

import (
	"fmt"
	"testing"
)

func TestMarshallingAssertMethods(t *testing.T) {
	cases := []struct {
		Name         string
		Serialized   map[string][]byte
		Deserialized interface{}
	}{{
		Name: "primitive_type",
		Serialized: map[string][]byte{
			"json": []byte("1"),
			"yaml": []byte("1\n"),
		},
		Deserialized: 1,
	},
	}

	marshallers := []struct {
		Name       string
		Marshaller Marshaler
	}{{
		Name:       "json",
		Marshaller: JSONMarshaler,
	}, {
		Name:       "yaml",
		Marshaller: YAMLMarshaler,
	}}

	for _, tc := range cases {
		for _, marshaller := range marshallers {
			serialized, ok := tc.Serialized[marshaller.Name]
			if !ok {
				// skip this marshaller
				continue
			}

			t.Run(fmt.Sprintf("%s/%s", tc.Name, marshaller.Name), func(t *testing.T) {
				AssertMarshals(t, marshaller.Marshaller, tc.Deserialized, serialized)
				AssertUnmarshals(t, marshaller.Marshaller, tc.Deserialized, serialized)
				AssertMarshalingRoundtrips(t, marshaller.Marshaller, serialized)
			})
		}
	}
}
