package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

// LogicalPlan converts a DAG into a list of steps to be executed in order
type LogicalPlan struct {
	Steps    map[parser.TransformID]LogicalStep
	Pipeline []parser.TransformID // Ordered list of steps to be performed
}

// LogicalStep is a single step in a logical plan
type LogicalStep struct {
	Parents   []parser.TransformID
	Children  []parser.TransformID
	Transform parser.Transform
}

// NewLogicalPlan creates a plan from the DAG structure
func NewLogicalPlan(transforms parser.Transforms, edges parser.Edges) (LogicalPlan, error) {
	lp := LogicalPlan{
		Steps:    make(map[parser.TransformID]LogicalStep),
		Pipeline: make([]parser.TransformID, 0, len(transforms)),
	}

	// Create all steps
	for _, transform := range transforms {
		lp.Steps[transform.ID] = LogicalStep{
			Transform: transform,
			Parents:   make([]parser.TransformID, 0, 1),
			Children:  make([]parser.TransformID, 0, 1),
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
	steps := make(map[parser.TransformID]LogicalStep)
	for id, step := range l.Steps {
		steps[id] = step.Clone()
	}

	pipeline := make([]parser.TransformID, len(l.Pipeline))
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
func (l LogicalStep) ID() parser.TransformID {
	return l.Transform.ID
}

// Clone the step, the transform is immutable so its left as is
func (l LogicalStep) Clone() LogicalStep {
	parents := make([]parser.TransformID, len(l.Parents))
	copy(parents, l.Parents)

	children := make([]parser.TransformID, len(l.Children))
	copy(children, l.Children)

	return LogicalStep{
		Transform: l.Transform,
		Parents:   parents,
		Children:  children,
	}
}
