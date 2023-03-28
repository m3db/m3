// go:generate stringer -type=Type
// Copyright (c) 2017 Uber Technologies, Inc.
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

package transformation

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
)

// Type defines a transformation function.
type Type int32

var errUnknownTransformationType = errors.New("unknown transformation type")

// Supported transformation types.
const (
	UnknownType Type = iota
	Absolute
	PerSecond
	Increase
	Add
	Reset
)

const (
	_minValidTransformationType = Absolute
	_maxValidTransformationType = Reset
)

// IsValid checks if the transformation type is valid.
func (t Type) IsValid() bool {
	return t.IsUnaryTransform() || t.IsBinaryTransform() || t.IsUnaryMultiOutputTransform()
}

// IsUnaryTransform returns whether this is a unary transformation.
func (t Type) IsUnaryTransform() bool {
	_, exists := unaryTransforms[t]
	return exists
}

// IsBinaryTransform returns whether this is a binary transformation.
func (t Type) IsBinaryTransform() bool {
	_, exists := binaryTransforms[t]
	return exists
}

func (t Type) IsUnaryMultiOutputTransform() bool {
	_, exists := unaryMultiOutputTransforms[t]
	return exists
}

// NewOp returns a constructed operation that is allocated once and can be
// reused.
func (t Type) NewOp() (Op, error) {
	var (
		err        error
		unary      UnaryTransform
		binary     BinaryTransform
		unaryMulti UnaryMultiOutputTransform
	)
	switch {
	case t.IsUnaryTransform():
		unary, err = t.UnaryTransform()
	case t.IsBinaryTransform():
		binary, err = t.BinaryTransform()
	case t.IsUnaryMultiOutputTransform():
		unaryMulti, err = t.UnaryMultiOutputTransform()
	default:
		err = errUnknownTransformationType
	}
	if err != nil {
		return Op{}, err
	}
	return Op{
		opType:     t,
		unary:      unary,
		binary:     binary,
		unaryMulti: unaryMulti,
	}, nil
}

// UnaryTransform returns the unary transformation function associated with
// the transformation type if applicable, or an error otherwise.
func (t Type) UnaryTransform() (UnaryTransform, error) {
	tf, exists := unaryTransforms[t]
	if !exists {
		return nil, fmt.Errorf("%v is not a unary transfomration", t)
	}
	return tf(), nil
}

// MustUnaryTransform returns the unary transformation function associated with
// the transformation type if applicable, or panics otherwise.
func (t Type) MustUnaryTransform() UnaryTransform {
	tf, err := t.UnaryTransform()
	if err != nil {
		panic(err)
	}
	return tf
}

// BinaryTransform returns the binary transformation function associated with
// the transformation type if applicable, or an error otherwise.
func (t Type) BinaryTransform() (BinaryTransform, error) {
	tf, exists := binaryTransforms[t]
	if !exists {
		return nil, fmt.Errorf("%v is not a binary transfomration", t)
	}
	return tf(), nil
}

// MustBinaryTransform returns the binary transformation function associated with
// the transformation type if applicable, or an error otherwise.
func (t Type) MustBinaryTransform() BinaryTransform {
	tf, err := t.BinaryTransform()
	if err != nil {
		panic(err)
	}
	return tf
}

// UnaryMultiOutputTransform returns the unary transformation function associated with
// the transformation type if applicable, or an error otherwise.
func (t Type) UnaryMultiOutputTransform() (UnaryMultiOutputTransform, error) {
	tf, exists := unaryMultiOutputTransforms[t]
	if !exists {
		return nil, fmt.Errorf("%v is not a unary transfomration", t)
	}
	return tf(), nil
}

// MustUnaryMultiOutputTransform returns the unary transformation function associated with
// the transformation type if applicable, or panics otherwise.
func (t Type) MustUnaryMultiOutputTransform() UnaryMultiOutputTransform {
	tf, err := t.UnaryMultiOutputTransform()
	if err != nil {
		panic(err)
	}
	return tf
}

// ToProto converts the transformation type to a protobuf message in place.
func (t Type) ToProto(pb *transformationpb.TransformationType) error {
	if t < _minValidTransformationType || t > _maxValidTransformationType {
		return errUnknownTransformationType
	}
	*pb = transformationpb.TransformationType(t)
	return nil
}

// FromProto converts the protobuf message to a transformation type in place.
func (t *Type) FromProto(pb transformationpb.TransformationType) error {
	*t = Type(pb)
	if *t < _minValidTransformationType || *t > _maxValidTransformationType {
		return errUnknownTransformationType
	}
	return nil
}

// UnmarshalText extracts this type from the textual representation
func (t *Type) UnmarshalText(text []byte) error {
	parsed, err := ParseType(string(text))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// MarshalYAML marshals a Type.
func (t Type) MarshalYAML() (interface{}, error) {
	return t.String(), nil
}

// UnmarshalYAML unmarshals text-encoded data into an transformation type.
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	value, err := ParseType(str)
	if err != nil {
		return err
	}
	*t = value
	return nil
}

// MarshalText serializes this type to its textual representation.
func (t Type) MarshalText() (text []byte, err error) {
	if !t.IsValid() {
		return nil, fmt.Errorf("invalid aggregation type %s", t.String())
	}
	return []byte(t.String()), nil
}

// ParseType parses a transformation type.
func ParseType(str string) (Type, error) {
	t, ok := typeStringMap[str]
	if !ok {
		return UnknownType, fmt.Errorf("invalid transformation type: %s", str)
	}
	return t, nil
}

// Op represents a transform operation.
type Op struct {
	unary      UnaryTransform
	binary     BinaryTransform
	unaryMulti UnaryMultiOutputTransform
	// opType determines which one of the above transformations are applied
	opType Type
}

// Type returns the op type.
func (o Op) Type() Type {
	return o.opType
}

// UnaryTransform returns the active unary transform if op is unary transform.
func (o Op) UnaryTransform() (UnaryTransform, bool) {
	if !o.Type().IsUnaryTransform() {
		return nil, false
	}
	return o.unary, true
}

// BinaryTransform returns the active binary transform if op is binary transform.
func (o Op) BinaryTransform() (BinaryTransform, bool) {
	if !o.Type().IsBinaryTransform() {
		return nil, false
	}
	return o.binary, true
}

// UnaryMultiOutputTransform returns the active unary multi transform if op is unary multi transform.
func (o Op) UnaryMultiOutputTransform() (UnaryMultiOutputTransform, bool) {
	if !o.Type().IsUnaryMultiOutputTransform() {
		return nil, false
	}
	return o.unaryMulti, true
}

var (
	unaryTransforms = map[Type]func() UnaryTransform{
		Absolute: transformAbsolute,
		Add:      transformAdd,
	}
	binaryTransforms = map[Type]func() BinaryTransform{
		PerSecond: transformPerSecond,
		Increase:  transformIncrease,
	}
	unaryMultiOutputTransforms = map[Type]func() UnaryMultiOutputTransform{
		Reset: transformReset,
	}
	typeStringMap map[string]Type
)

func init() {
	typeStringMap = make(map[string]Type)
	for t := range unaryTransforms {
		typeStringMap[t.String()] = t
	}
	for t := range binaryTransforms {
		typeStringMap[t.String()] = t
	}
	for t := range unaryMultiOutputTransforms {
		typeStringMap[t.String()] = t
	}
}
