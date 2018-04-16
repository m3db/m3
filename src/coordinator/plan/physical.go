package plan

import (
	"fmt"

	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
)

// PhysicalPlan represents the physical plan
type PhysicalPlan struct {
	steps      map[parser.TransformID]LogicalStep
	pipeline   []parser.TransformID // Ordered list of steps to be performed
	resultStep LogicalStep
}

// NewPhysicalPlan is used to generate a physical plan. Its responsibilities include creating consolidation nodes, result nodes,
// pushing down predicates, changing the ordering for nodes
func NewPhysicalPlan(lp LogicalPlan, storage storage.Storage) (PhysicalPlan, error) {
	// generate a new physical plan after cloning the logical plan so that any changes here do not update the logical plan
	cloned := lp.Clone()
	p := PhysicalPlan{
		steps:    cloned.Steps,
		pipeline: cloned.Pipeline,
	}

	pl, err := p.createResultNode()
	if err != nil {
		return PhysicalPlan{}, err
	}

	return pl, nil
}

func (p PhysicalPlan) createResultNode() (PhysicalPlan, error) {
	leaf, err := p.leafNode()
	if err != nil {
		return p, err
	}

	resultNode := parser.NewTransformFromOperation(&ResultOp{}, len(p.steps)+1)
	resultStep := LogicalStep{
		Transform: resultNode,
		Parents:   []parser.TransformID{leaf.ID()},
		Children:  []parser.TransformID{},
	}

	p.resultStep = resultStep
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

func (p PhysicalPlan) String() string {
	return fmt.Sprintf("Steps: %s, Pipeline: %s, Result: %s", p.steps, p.pipeline, p.resultStep)
}
