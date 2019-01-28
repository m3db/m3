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

package plan

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
)

// PhysicalPlan represents the physical plan
type PhysicalPlan struct {
	steps      map[parser.NodeID]LogicalStep
	pipeline   []parser.NodeID // Ordered list of steps to be performed
	ResultStep ResultOp
	TimeSpec   transform.TimeSpec
	Debug      bool
	BlockType  models.FetchedBlockType
}

// ResultOp is resonsible for delivering results to the clients
type ResultOp struct {
	Parent parser.NodeID
}

// NewPhysicalPlan is used to generate a physical plan. Its responsibilities include creating consolidation nodes, result nodes,
// pushing down predicates, changing the ordering for nodes
// nolint: unparam
func NewPhysicalPlan(lp LogicalPlan, storage storage.Storage, params models.RequestParams) (PhysicalPlan, error) {
	// generate a new physical plan after cloning the logical plan so that any changes here do not update the logical plan
	cloned := lp.Clone()
	p := PhysicalPlan{
		steps:    cloned.Steps,
		pipeline: cloned.Pipeline,
		TimeSpec: transform.TimeSpec{
			Start: params.Start,
			End:   params.ExclusiveEnd(),
			Now:   params.Now,
			Step:  params.Step,
		},
		Debug:     params.Debug,
		BlockType: params.BlockType,
	}

	pl, err := p.createResultNode()
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Update times
	pl = pl.shiftTime()
	return pl, nil
}

func (p PhysicalPlan) shiftTime() PhysicalPlan {
	var maxRange time.Duration
	// Start offset with lookback
	maxOffset := models.LookbackDelta
	for _, transformID := range p.pipeline {
		node := p.steps[transformID]
		boundOp, ok := node.Transform.Op.(transform.BoundOp)
		if !ok {
			continue
		}

		spec := boundOp.Bounds()
		if spec.Offset+models.LookbackDelta > maxOffset {
			maxOffset = spec.Offset + models.LookbackDelta
		}

		if spec.Range > maxRange {
			maxRange = spec.Range
		}
	}

	startShift := maxOffset + maxRange
	// keeping end the same for now, might optimize later
	p.TimeSpec.Start = p.TimeSpec.Start.Add(-1 * startShift)
	return p
}

func (p PhysicalPlan) createResultNode() (PhysicalPlan, error) {
	leaf, err := p.leafNode()
	if err != nil {
		return p, err
	}

	p.ResultStep = ResultOp{Parent: leaf.ID()}
	return p, nil
}

func (p PhysicalPlan) leafNode() (LogicalStep, error) {
	var leaf LogicalStep
	found := false
	for _, transformID := range p.pipeline {
		node, ok := p.steps[transformID]
		if !ok {
			return leaf, fmt.Errorf("transform not found, %s", transformID)
		}

		if len(node.Children) == 0 {
			if found {
				return leaf, fmt.Errorf("multiple leaf nodes found, %v - %v", leaf, node)
			}

			leaf = node
			found = true
		}
	}

	return leaf, nil
}

// Step gets the logical step using its unique ID in the DAG
func (p PhysicalPlan) Step(ID parser.NodeID) (LogicalStep, bool) {
	// Editor complains when inlining the map get
	step, ok := p.steps[ID]
	return step, ok
}

// String representation of the physical plan
func (p PhysicalPlan) String() string {
	return fmt.Sprintf("StepCount: %s, Pipeline: %s, Result: %s, TimeSpec: %v", p.steps, p.pipeline, p.ResultStep, p.TimeSpec)
}
