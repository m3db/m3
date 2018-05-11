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

	"github.com/m3db/m3db/src/coordinator/parser"
)

// LogicalPlan converts a DAG into a list of steps to be executed in order
type LogicalPlan struct {
	Steps    map[parser.NodeID]LogicalStep
	Pipeline []parser.NodeID // Ordered list of steps to be performed
}

// LogicalStep is a single step in a logical plan
type LogicalStep struct {
	Parents   []parser.NodeID
	Children  []parser.NodeID
	Transform parser.Node
}

// NewLogicalPlan creates a plan from the DAG structure
func NewLogicalPlan(transforms parser.Nodes, edges parser.Edges) (LogicalPlan, error) {
	lp := LogicalPlan{
		Steps:    make(map[parser.NodeID]LogicalStep),
		Pipeline: make([]parser.NodeID, 0, len(transforms)),
	}

	// Create all steps
	for _, transform := range transforms {
		lp.Steps[transform.ID] = LogicalStep{
			Transform: transform,
			Parents:   make([]parser.NodeID, 0, 1),
			Children:  make([]parser.NodeID, 0, 1),
		}
		lp.Pipeline = append(lp.Pipeline, transform.ID)
	}

	// Link all parent/children
	for _, edge := range edges {
		parent, ok := lp.Steps[edge.ParentID]
		if !ok {
			return LogicalPlan{}, fmt.Errorf("invalid DAG found, parent %s not found for child %s", edge.ParentID, edge.ChildID)
		}

		child, ok := lp.Steps[edge.ChildID]
		if !ok {
			return LogicalPlan{}, fmt.Errorf("invalid DAG found, child %s not found for parent %s", edge.ChildID, edge.ParentID)
		}

		parent.Children = append(parent.Children, child.ID())
		child.Parents = append(child.Parents, parent.ID())
		// Write back since we are doing copy instead reference
		lp.Steps[edge.ParentID] = parent
		lp.Steps[edge.ChildID] = child
	}

	return lp, nil
}

func (l LogicalPlan) String() string {
	return fmt.Sprintf("Steps: %s, Pipeline: %s", l.Steps, l.Pipeline)
}

// Clone the plan
func (l LogicalPlan) Clone() LogicalPlan {
	steps := make(map[parser.NodeID]LogicalStep)
	for id, step := range l.Steps {
		steps[id] = step.Clone()
	}

	pipeline := make([]parser.NodeID, len(l.Pipeline))
	copy(pipeline, l.Pipeline)
	return LogicalPlan{
		Steps:    steps,
		Pipeline: pipeline,
	}
}

func (l LogicalStep) String() string {
	return fmt.Sprintf("Parents: %s, Children: %s, Transform: %s", l.Parents, l.Children, l.Transform)
}

// ID is a convenience method to expose the inner transforms' ID
func (l LogicalStep) ID() parser.NodeID {
	return l.Transform.ID
}

// Clone the step, the transform is immutable so its left as is
func (l LogicalStep) Clone() LogicalStep {
	parents := make([]parser.NodeID, len(l.Parents))
	copy(parents, l.Parents)

	children := make([]parser.NodeID, len(l.Children))
	copy(children, l.Children)

	return LogicalStep{
		Transform: l.Transform,
		Parents:   parents,
		Children:  children,
	}
}
