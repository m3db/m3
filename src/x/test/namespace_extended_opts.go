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

func init() {
	namespace.RegisterExtendedOptionsConverter(TypeURLPrefix, &m3test.PingResponse{}, ConvertToExtendedOptions)
}

// NewProtobufAny converts a typed protobuf message into protobuf Any type.
func NewProtobufAny(msg proto.Message) *protobuftypes.Any {
	serializedMsg, _ := proto.Marshal(msg)
	return &protobuftypes.Any{
		TypeUrl: TypeURLPrefix + proto.MessageName(msg),
		Value:   serializedMsg,
	}
}

// NewExtendedOptionsProto construct a new protobuf Any message with ExtendedOptions.
func NewExtendedOptionsProto(value string) *protobuftypes.Any {
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
