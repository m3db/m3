package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
)

// LogicalPlan converts a DAG into a list of steps to be executed in order
type LogicalPlan struct {
	Transforms map[parser.TransformID]*LogicalStep
	Pipeline   []parser.TransformID // Ordered list of steps to be performed
}

// LogicalStep is a single step in a logical plan
type LogicalStep struct {
	Parents   []parser.TransformID
	Children  []parser.TransformID
	Transform *parser.Transform
}

// NewLogicalPlan returns an empty logical plan
func NewLogicalPlan() *LogicalPlan {
	return &LogicalPlan{
		Transforms: make(map[parser.TransformID]*LogicalStep),
		Pipeline:   make([]parser.TransformID, 0),
	}
}

// NewLogicalStep returns an empty plan step
func NewLogicalStep(Transform *parser.Transform) *LogicalStep {
	return &LogicalStep{
		Transform: Transform,
		Parents:   make([]parser.TransformID, 0),
		Children:  make([]parser.TransformID, 0),
	}
}

func (l *LogicalPlan) String() string {
	return fmt.Sprintf("Transforms: %s, Pipeline: %s", l.Transforms, l.Pipeline)
}

func (l *LogicalStep) String() string {
	return fmt.Sprintf("Parents: %s, Children: %s, Transform: %s", l.Parents, l.Children, l.Transform)
}

// GenerateLogicalPlan creates a plan from the DAG structure
func GenerateLogicalPlan(transforms parser.Transforms, edges parser.Edges) (*LogicalPlan, error) {
	lp := NewLogicalPlan()

	// Create all steps
	for _, transform := range transforms {
		lp.Transforms[transform.ID] = NewLogicalStep(transform)
		lp.Pipeline = append(lp.Pipeline, transform.ID)
	}

	// Link all parent/children
	for _, edge := range edges {
		parent, ok := lp.Transforms[edge.ParentID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, parent %s not found", edge.ParentID)
		}

		child, ok := lp.Transforms[edge.ChildID]
		if !ok {
			return nil, fmt.Errorf("invalid DAG found, child %s not found", edge.ChildID)
		}

		parent.Children = append(parent.Children, child.Transform.ID)
		child.Parents = append(child.Parents, parent.Transform.ID)
	}

	return lp, nil
}
