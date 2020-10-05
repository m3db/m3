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

	"github.com/m3db/m3/src/dbnode/namespace"
	m3test "github.com/m3db/m3/src/x/generated/proto/test"
	xjson "github.com/m3db/m3/src/x/json"

	"github.com/gogo/protobuf/proto"
	protobuftypes "github.com/gogo/protobuf/types"
)

// TypeURLPrefix is a type URL prefix for storing in protobuf Any messages.
const TypeURLPrefix = "testm3db.io/"

// ExtendedOptions is a struct for testing namespace ExtendedOptions.
type ExtendedOptions struct {
	value string
}

func (o *ExtendedOptions) Validate() error {
	if o.value == "invalid" {
		return errors.New("invalid ExtendedOptions")
	}
	return nil
}

func (o *ExtendedOptions) ToProto() (proto.Message, string) {
	return &m3test.PingResponse{Value: o.value}, TypeURLPrefix
}

// ConvertToExtendedOptions converts protobuf message to ExtendedOptions.
func ConvertToExtendedOptions(msg proto.Message) (namespace.ExtendedOptions, error) {
	typedMsg := msg.(*m3test.PingResponse)
	if typedMsg.Value == "error" {
		return nil, errors.New("error in converter")
	}
	return &ExtendedOptions{typedMsg.Value}, nil
}

// NewProtobufAny converts a typed protobuf message into protobuf Any type.
func NewProtobufAny(msg proto.Message) (*protobuftypes.Any, error) {
	serializedMsg, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &protobuftypes.Any{
		TypeUrl: TypeURLPrefix + proto.MessageName(msg),
		Value:   serializedMsg,
	}, nil
}

// NewExtendedOptionsProto construct a new protobuf Any message with ExtendedOptions.
func NewExtendedOptionsProto(value string) (*protobuftypes.Any, error) {
	// NB: using some arbitrary custom protobuf message to avoid well known protobuf types as these work across
	// gogo/golang implementations.
	msg := &m3test.PingResponse{Value: value}
	return NewProtobufAny(msg)
}

// NewExtendedOptionsJson returns a json Map with ExtendedOptions as protobuf Any.
func NewExtendedOptionsJson(value string) xjson.Map {
	return xjson.Map{
		"@type": TypeURLPrefix + proto.MessageName(&m3test.PingResponse{}),
		"Value": value,
		"counter": 0,
	}
}
