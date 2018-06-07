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
	"fmt"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/op"
)

var (
	// DefaultPipeline is a default pipeline.
	DefaultPipeline Pipeline
)

// Rollup captures the rollup metadata after the operation is applied against a metric ID.
type Rollup struct {
	// Metric ID generated as a result of the rollup.
	ID []byte
	// Type of aggregations performed within each unique dimension combination.
	AggregationID aggregation.ID
}

// Equal determines whether two rollup operations are equal.
func (op Rollup) Equal(other Rollup) bool {
	return op.AggregationID == other.AggregationID && bytes.Equal(op.ID, other.ID)
}

// Clone clones the rollup operation.
func (op Rollup) Clone() Rollup {
	idClone := make([]byte, len(op.ID))
	copy(idClone, op.ID)
	return Rollup{ID: idClone, AggregationID: op.AggregationID}
}

func (op Rollup) String() string {
	return fmt.Sprintf("{id: %s, aggregation: %v}", op.ID, op.AggregationID)
}

// Union is a union of different types of operation.
type Union struct {
	Type           op.Type
	Transformation op.Transformation
	Rollup         Rollup
}

// Equal determines whether two operation unions are equal.
func (u Union) Equal(other Union) bool {
	if u.Type != other.Type {
		return false
	}
	switch u.Type {
	case op.TransformationType:
		return u.Transformation.Equal(other.Transformation)
	case op.RollupType:
		return u.Rollup.Equal(other.Rollup)
	}
	return true
}

// Clone clones an operation union.
func (u Union) Clone() Union {
	clone := Union{Type: u.Type}
	switch u.Type {
	case op.TransformationType:
		clone.Transformation = u.Transformation.Clone()
	case op.RollupType:
		clone.Rollup = u.Rollup.Clone()
	}
	return clone
}

func (u Union) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	switch u.Type {
	case op.TransformationType:
		fmt.Fprintf(&b, "transformation: %s", u.Transformation.String())
	case op.RollupType:
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
	operations []Union
}

// NewPipeline creates a new pipeline.
func NewPipeline(ops []Union) Pipeline {
	return Pipeline{operations: ops}
}

// NumSteps returns the number of steps in a pipeline.
func (p Pipeline) NumSteps() int { return len(p.operations) }

// IsEmpty determines whether a pipeline is empty.
func (p Pipeline) IsEmpty() bool { return p.NumSteps() == 0 }

// At returns the operation at a given step.
func (p Pipeline) At(i int) Union { return p.operations[i] }

// Equal determines whether two pipelines are equal.
func (p Pipeline) Equal(other Pipeline) bool {
	if len(p.operations) != len(other.operations) {
		return false
	}
	for i := 0; i < len(p.operations); i++ {
		if !p.operations[i].Equal(other.operations[i]) {
			return false
		}
	}
	return true
}

// Clone clones the pipeline.
func (p Pipeline) Clone() Pipeline {
	clone := make([]Union, len(p.operations))
	for i, op := range p.operations {
		clone[i] = op.Clone()
	}
	return Pipeline{operations: clone}
}

// SubPipeline returns a sub-pipeline starting from step `idx`
// of the current pipeline.
func (p Pipeline) SubPipeline(idx int) Pipeline {
	return Pipeline{operations: p.operations[idx:]}
}

func (p Pipeline) String() string {
	var b bytes.Buffer
	b.WriteString("{operations: [")
	for i, op := range p.operations {
		b.WriteString(op.String())
		if i < len(p.operations)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]}")
	return b.String()
}
