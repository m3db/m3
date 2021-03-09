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

package applied

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/transformation"
)

var (
	// DefaultPipeline is a default pipeline.
	DefaultPipeline Pipeline

	errNilAppliedRollupOpProto  = errors.New("nil applied rollup op proto message")
	errUnknownOpType            = errors.New("unknown op type")
	errOperationsLengthMismatch = errors.New("operations list length does not match proto")
	errNilTransformationOpProto = errors.New("nil transformation op proto message")
)

// RollupOp captures the rollup metadata after the operation is applied against a metric ID.
type RollupOp struct {
	// Metric ID generated as a result of the rollup.
	ID []byte
	// Type of aggregations performed within each unique dimension combination.
	AggregationID aggregation.ID
}

// Equal determines whether two rollup Operations are equal.
func (op RollupOp) Equal(other RollupOp) bool {
	return op.AggregationID == other.AggregationID && bytes.Equal(op.ID, other.ID)
}

// Clone clones the rollup operation.
func (op RollupOp) Clone() RollupOp {
	idClone := make([]byte, len(op.ID))
	copy(idClone, op.ID)
	return RollupOp{ID: idClone, AggregationID: op.AggregationID}
}

func (op RollupOp) String() string {
	return fmt.Sprintf("{id: %s, aggregation: %v}", op.ID, op.AggregationID)
}

// ToProto converts the applied rollup op to a protobuf message in place.
func (op RollupOp) ToProto(pb *pipelinepb.AppliedRollupOp) error {
	op.AggregationID.ToProto(&pb.AggregationId)
	pb.Id = op.ID
	return nil
}

// FromProto converts the protobuf message to an applied rollup op in place.
func (op *RollupOp) FromProto(pb *pipelinepb.AppliedRollupOp) error {
	if pb == nil {
		return errNilAppliedRollupOpProto
	}
	op.AggregationID.FromProto(pb.AggregationId)
	op.ID = pb.Id
	return nil
}

// OpUnion is a union of different types of operation.
type OpUnion struct {
	Type           pipeline.OpType
	Transformation pipeline.TransformationOp
	Rollup         RollupOp
}

// Equal determines whether two operation unions are equal.
func (u OpUnion) Equal(other OpUnion) bool {
	// keep in sync with Pipeline.Equal as go is terrible at inlining anything with a loop
	if u.Type != other.Type {
		return false
	}

	if u.Type == pipeline.RollupOpType {
		return u.Rollup.Equal(other.Rollup)
	}

	return u.Transformation.Type == other.Transformation.Type
}

// Clone clones an operation union.
func (u OpUnion) Clone() OpUnion {
	clone := OpUnion{
		Type:           u.Type,
		Transformation: u.Transformation,
	}
	if u.Type == pipeline.RollupOpType {
		clone.Rollup = u.Rollup.Clone()
	}
	return clone
}

func (u OpUnion) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case pipeline.TransformationOpType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case pipeline.RollupOpType:
		fmt.Fprintf(&b, "rollup: %s", u.Rollup.String())
	default:
		fmt.Fprintf(&b, "unknown op type: %v", u.Type)
	}
	b.WriteString("}")
	return b.String()
}

// ToProto converts the applied pipeline op to a protobuf message in place.
func (u OpUnion) ToProto(pb *pipelinepb.AppliedPipelineOp) error {
	pb.Reset()
	switch u.Type {
	case pipeline.TransformationOpType:
		pb.Type = pipelinepb.AppliedPipelineOp_TRANSFORMATION
		pb.Transformation = &pipelinepb.TransformationOp{}
		return u.Transformation.ToProto(pb.Transformation)
	case pipeline.RollupOpType:
		pb.Type = pipelinepb.AppliedPipelineOp_ROLLUP
		pb.Rollup = &pipelinepb.AppliedRollupOp{}
		return u.Rollup.ToProto(pb.Rollup)
	default:
		return errUnknownOpType
	}
}

// Reset resets the operation union.
func (u *OpUnion) Reset() { *u = OpUnion{} }

// FromProto converts the protobuf message to an applied pipeline op in place.
func (u *OpUnion) FromProto(pb pipelinepb.AppliedPipelineOp) error {
	switch pb.Type {
	case pipelinepb.AppliedPipelineOp_TRANSFORMATION:
		u.Type = pipeline.TransformationOpType
		if u.Rollup.ID != nil {
			u.Rollup.ID = u.Rollup.ID[:0]
		}
		u.Rollup.AggregationID[0] = aggregation.DefaultID[0]
		return u.Transformation.FromProto(pb.Transformation)
	case pipelinepb.AppliedPipelineOp_ROLLUP:
		u.Type = pipeline.RollupOpType
		u.Transformation.Type = transformation.UnknownType
		return u.Rollup.FromProto(pb.Rollup)
	default:
		return errUnknownOpType
	}
}

// Pipeline is a pipeline of operations.
type Pipeline struct {
	// a list of pipeline Operations.
	Operations []OpUnion
}

// NewPipeline creates a new pipeline.
func NewPipeline(ops []OpUnion) Pipeline {
	return Pipeline{Operations: ops}
}

// Len returns the number of steps in a pipeline.
func (p Pipeline) Len() int { return len(p.Operations) }

// IsEmpty determines whether a pipeline is empty.
func (p Pipeline) IsEmpty() bool { return len(p.Operations) == 0 }

// At returns the operation at a given step.
func (p Pipeline) At(i int) OpUnion { return p.Operations[i] }

// Equal determines whether two pipelines are equal.
func (p Pipeline) Equal(other Pipeline) bool {
	// keep in sync with OpUnion.Equal as go is terrible at inlining anything with a loop
	if len(p.Operations) != len(other.Operations) {
		return false
	}

	for i := 0; i < len(p.Operations); i++ {
		if p.Operations[i].Type != other.Operations[i].Type {
			return false
		}
		//nolint:exhaustive
		switch p.Operations[i].Type {
		case pipeline.RollupOpType:
			if !p.Operations[i].Rollup.Equal(other.Operations[i].Rollup) {
				return false
			}
		case pipeline.TransformationOpType:
			if p.Operations[i].Transformation.Type != other.Operations[i].Transformation.Type {
				return false
			}
		}
	}

	return true
}

// Clone clones the pipeline.
func (p Pipeline) Clone() Pipeline {
	clone := make([]OpUnion, len(p.Operations))
	for i := range p.Operations {
		clone[i] = p.Operations[i].Clone()
	}
	return Pipeline{Operations: clone}
}

// SubPipeline returns a sub-pipeline containing Operations between step `startInclusive`
// and step `endExclusive` of the current pipeline.
func (p Pipeline) SubPipeline(startInclusive int, endExclusive int) Pipeline {
	return Pipeline{Operations: p.Operations[startInclusive:endExclusive]}
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

// ToProto converts the applied pipeline to a protobuf message in place.
func (p Pipeline) ToProto(pb *pipelinepb.AppliedPipeline) error {
	numOps := len(p.Operations)
	if cap(pb.Ops) >= numOps {
		pb.Ops = pb.Ops[:numOps]
	} else {
		pb.Ops = make([]pipelinepb.AppliedPipelineOp, numOps)
	}
	for i := 0; i < numOps; i++ {
		if err := p.Operations[i].ToProto(&pb.Ops[i]); err != nil {
			return err
		}
	}
	return nil
}

// FromProto converts the protobuf message to an applied pipeline in place.
func (p *Pipeline) FromProto(pb pipelinepb.AppliedPipeline) error {
	numOps := len(pb.Ops)
	if cap(p.Operations) >= numOps {
		p.Operations = p.Operations[:numOps]
	} else {
		p.Operations = make([]OpUnion, numOps)
	}
	for i := 0; i < numOps; i++ {
		if err := p.Operations[i].FromProto(pb.Ops[i]); err != nil {
			return err
		}
	}
	return nil
}

// IsMappingRule returns whether this is a mapping rule, determined by
// if any rollup pipelines are included.
func (p Pipeline) IsMappingRule() bool {
	for _, op := range p.Operations {
		if op.Rollup.ID != nil {
			return false
		}
	}
	return true
}

// OperationsFromProto converts a list of protobuf AppliedPipelineOps, used in optimized staged metadata methods.
func OperationsFromProto(pb []pipelinepb.AppliedPipelineOp, ops []OpUnion) error {
	numOps := len(pb)
	if numOps != len(ops) {
		return errOperationsLengthMismatch
	}
	for i := 0; i < numOps; i++ {
		u := &ops[i]
		u.Type = pipeline.OpType(pb[i].Type + 1)
		switch u.Type {
		case pipeline.TransformationOpType:
			if u.Rollup.ID != nil {
				u.Rollup.ID = u.Rollup.ID[:0]
			}
			u.Rollup.AggregationID[0] = aggregation.DefaultID[0]
			if pb[i].Transformation == nil {
				return errNilTransformationOpProto
			}
			if err := u.Transformation.Type.FromProto(pb[i].Transformation.Type); err != nil {
				return err
			}
		case pipeline.RollupOpType:
			u.Transformation.Type = transformation.UnknownType
			if pb == nil {
				return errNilAppliedRollupOpProto
			}
			u.Rollup.AggregationID[0] = pb[i].Rollup.AggregationId.Id
			u.Rollup.ID = pb[i].Rollup.Id
		default:
			return errUnknownOpType
		}
	}
	return nil
}
