// Copyright (c) 2018 Uber Technologies, Inc.
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

package database

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jhump/protoreflect/dynamic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDynamic(t *testing.T) {
	m := dynamic.Message{}
	err := m.UnmarshalJSON([]byte(`{"key":"foo"}`))
	fmt.Println(m)
	fmt.Println(err)
}

func TestDynamic2(t *testing.T) {
	m := map[string]interface{}{
		"foo": "bar",
		"baz": 123,
	}
	b, err := json.Marshal(m)
	require.NoError(t, err)
	fmt.Println(string(b))
	s := &structpb.Struct{}
	err = protojson.Unmarshal([]byte(`{"key":"foo"}`), s)
	require.NoError(t, err)
	fmt.Println(m)
	fmt.Println(b)
	fmt.Println(s)

	// v := &commonpb.StringProto{}
	// err = protojson.Unmarshal([]byte(`{"value":"foo"}`), v)
	// require.NoError(t, err)
	// fmt.Println(v)
}
