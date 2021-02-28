// Copyright (c) 2020 Uber Technologies, Inc.
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

package test

import (
	"errors"
	"fmt"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	xjson "github.com/m3db/m3/src/x/json"

	protobuftypes "github.com/gogo/protobuf/types"
)

// TypeURLPrefix is a type URL prefix for storing in protobuf Any messages.
const TypeURLPrefix = "testm3db.io/"

type testExtendedOptions struct {
	value string
}

func (o *testExtendedOptions) Validate() error {
	if o.value == "invalid" {
		return errors.New("invalid ExtendedOptions")
	}
	return nil
}

func (o *testExtendedOptions) ToProto() (string, *protobuftypes.Struct) {
	extOptsProto := NewTestExtendedOptionsProto(o.value)
	return extOptsProto.Type, extOptsProto.Options
}

// ConvertToTestExtendedOptions is ExtendedOptsConverter for testExtendedOptions.
func ConvertToTestExtendedOptions(
	opts *protobuftypes.Struct,
) (namespace.ExtendedOptions, error) {
	value, ok := opts.Fields["value"]
	if !ok {
		return nil, fmt.Errorf("missing field 'value' in %s", opts)
	}
	stringValue := value.GetStringValue()
	if stringValue == "error" {
		return nil, errors.New("test error in converter")
	}

	return &testExtendedOptions{stringValue}, nil
}

// NewTestExtendedOptionsProto construct a new protobuf ExtendedOptions message.
func NewTestExtendedOptionsProto(value string) *nsproto.ExtendedOptions {
	options := map[string]*protobuftypes.Value{
		"value": {Kind: &protobuftypes.Value_StringValue{StringValue: value}},
	}

	return &nsproto.ExtendedOptions{
		Type:    "testExtendedOptions",
		Options: &protobuftypes.Struct{Fields: options},
	}
}

// NewTestExtendedOptionsJSON returns a json Map for testExtendedOptions.
func NewTestExtendedOptionsJSON(value string) xjson.Map {
	return xjson.Map{
		"type": "testExtendedOptions",
		"options": xjson.Map{
			"value": value,
		},
	}
}
