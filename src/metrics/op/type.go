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

package op

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/transformation"
)

var (
	errNilTransformationOpProto = errors.New("nil transformation op proto message")
)

// Type defines the type of an operation.
type Type int

// List of supported operation types.
const (
	UnknownType Type = iota
	AggregationType
	TransformationType
	RollupType
)

// Aggregation is an aggregation operation.
type Aggregation struct {
	// Type of aggregation performed.
	Type aggregation.Type
}

// Equal determines whether two aggregation operations are equal.
func (op Aggregation) Equal(other Aggregation) bool {
	return op.Type == other.Type
}

func (op Aggregation) String() string {
	return op.Type.String()
}

// Transformation is a transformation operation.
type Transformation struct {
	// Type of transformation performed.
	Type transformation.Type
}

// Equal determines whether two transformation operations are equal.
func (op Transformation) Equal(other Transformation) bool {
	return op.Type == other.Type
}

// Clone clones the transformation operation.
func (op Transformation) Clone() Transformation {
	return op
}

func (op Transformation) String() string {
	return op.Type.String()
}

// ToProto converts the transformation op to a protobuf message in place.
func (op Transformation) ToProto(pb *pipelinepb.TransformationOp) error {
	return op.Type.ToProto(&pb.Type)
}

// FromProto converts the protobuf message to a transformation in place.
func (op *Transformation) FromProto(pb *pipelinepb.TransformationOp) error {
	if pb == nil {
		return errNilTransformationOpProto
	}
	return op.Type.FromProto(pb.Type)
}

// Rollup is a rollup operation.
type Rollup struct {
	// New metric name generated as a result of the rollup.
	NewName []byte
	// Dimensions along which the rollup is performed.
	Tags [][]byte
	// Type of aggregation performed within each unique dimension combination.
	AggregationType aggregation.Type
}

func (op Rollup) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	fmt.Fprintf(&b, "name: %s, ", op.NewName)
	b.WriteString("tags: [")
	for i, t := range op.Tags {
		fmt.Fprintf(&b, "%s", t)
		if i < len(op.Tags)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("], ")
	fmt.Fprintf(&b, "aggregation: %v", op.AggregationType)
	b.WriteString("}")
	return b.String()
}

// Union is a union of different types of operation.
type Union struct {
	Type           Type
	Aggregation    Aggregation
	Transformation Transformation
	Rollup         Rollup
}

func (u Union) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case AggregationType:
		fmt.Fprintf(&b, "aggregation: %s", u.Aggregation.String())
	case TransformationType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case RollupType:
		fmt.Fprintf(&b, "rollup: %s", u.Rollup.String())
	default:
		fmt.Fprintf(&b, "unknown op type: %v", u.Type)
	}
	b.WriteString("}")
	return b.String()
}

// Pipeline is a pipeline of operations.
type Pipeline struct {
	// a list of pipeline operations.
	Operations []Union
}

func (p Pipeline) String() string {
	var b bytes.Buffer
	b.WriteString("{operations: [")
	for i, op := range p.Operations {
		b.WriteString(op.String())
		if i < len(p.Operations)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]}")
	return b.String()
}
