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
	"fmt"

	"github.com/m3db/m3metrics/generated/proto/transformationpb"
)

// Type defines a transformation function.
type Type int

// Supported transformation types.
const (
	UnknownType Type = iota
	Absolute
	PerSecond
)

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

// UnaryTransform returns the unary transformation function associated with
// the transformation type if applicable, or an error otherwise.
func (t Type) UnaryTransform() (UnaryTransform, error) {
	tf, exists := unaryTransforms[t]
	if !exists {
		return nil, fmt.Errorf("%v is not a unary transfomration", t)
	}
	return tf, nil
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
	return tf, nil
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

// ToProto converts the transformation type to a protobuf message in place.
func (t Type) ToProto(pb *transformationpb.TransformationType) error {
	switch t {
	case Absolute:
		*pb = transformationpb.TransformationType_ABSOLUTE
	case PerSecond:
		*pb = transformationpb.TransformationType_PERSECOND
	default:
		return fmt.Errorf("unknown transformation type: %v", t)
	}
	return nil
}

// FromProto converts the protobuf message to a transformation type in place.
func (t *Type) FromProto(pb transformationpb.TransformationType) error {
	switch pb {
	case transformationpb.TransformationType_ABSOLUTE:
		*t = Absolute
	case transformationpb.TransformationType_PERSECOND:
		*t = PerSecond
	default:
		return fmt.Errorf("unknown transformation type in proto: %v", pb)
	}
	return nil
}

var (
	unaryTransforms = map[Type]UnaryTransform{
		Absolute: absolute,
	}
	binaryTransforms = map[Type]BinaryTransform{
		PerSecond: perSecond,
	}
)
