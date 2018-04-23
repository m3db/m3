package executor

import (
	"context"
	"fmt"

	"github.com/m3db/m3coordinator/executor/transform"
	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/plan"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/execution"

	"github.com/pkg/errors"
)

// ExecutionState represents the execution hierarchy
type ExecutionState struct {
	plan       plan.PhysicalPlan
	sources    []parser.Source
	resultNode parser.OpNode
	storage    storage.Storage
}

// CreateSource creates a source node
func CreateSource(ID parser.NodeID, params SourceParams, storage storage.Storage) (parser.Source, *transform.Controller) {
	controller := &transform.Controller{ID: ID}
	return params.Node(controller, storage), controller
}

// CreateTransform creates a transform node which works on functions and contains state
func CreateTransform(ID parser.NodeID, params TransformParams) (parser.OpNode, *transform.Controller) {
	controller := &transform.Controller{ID: ID}
	return params.Node(controller), controller
}

// TransformParams are defined by transforms
type TransformParams interface {
	parser.Params
	Node(controller *transform.Controller) parser.OpNode
}

// SourceParams are defined by sources
type SourceParams interface {
	parser.Params
	Node(controller *transform.Controller, storage storage.Storage) parser.Source
}

// GenerateExecutionState creates an execution state from the physical plan
func GenerateExecutionState(pplan plan.PhysicalPlan, storage storage.Storage) (*ExecutionState, error) {
	result := pplan.ResultStep
	state := &ExecutionState{
		plan:    pplan,
		storage: storage,
	}

	step, ok := pplan.Step(result.Parent)
	if !ok {
		return nil, fmt.Errorf("incorrect parent reference in result node, parentId: %s", result.Parent)
	}

	controller, err := state.createNode(step)
	if err != nil {
		return nil, err
	}

	if len(state.sources) == 0 {
		return nil, errors.New("empty sources for the execution state")
	}

	state.resultNode = ResultNode{}
	controller.AddTransform(state.resultNode)

	return state, nil
}

// createNode helps to create an execution node recursively
// TODO: consider modifying this function so that ExecutionState can have a non pointer receiver
func (s *ExecutionState) createNode(step plan.LogicalStep) (*transform.Controller, error) {
	// TODO: consider using a registry instead of casting to an interface
	sourceParams, ok := step.Transform.Op.(SourceParams)
	if ok {
		source, controller := CreateSource(step.ID(), sourceParams, s.storage)
		s.sources = append(s.sources, source)
		return controller, nil
	}

	transformParams, ok := step.Transform.Op.(TransformParams)
	if !ok {
		return nil, fmt.Errorf("invalid transform step, %s", step)
	}

	transformNode, controller := CreateTransform(step.ID(), transformParams)
	for _, parentID := range step.Parents {
		parentStep, ok := s.plan.Step(parentID)
		if !ok {
			return nil, fmt.Errorf("incorrect parent reference, parentId: %s, node: %s", parentID, step.ID())
		}

		parentController, err := s.createNode(parentStep)
		if err != nil {
			return nil, err
		}

		parentController.AddTransform(transformNode)
	}

	return controller, nil
}

// Execute the sources in parallel and return the first error
func (s *ExecutionState) Execute(ctx context.Context) error {
	requests := make([]execution.Request, len(s.sources))
	for idx, source := range s.sources {
		requests[idx] = sourceRequest{source}
	}

	return execution.ExecuteParallel(ctx, requests)
}

// String representation of the state
func (s *ExecutionState) String() string {
	return fmt.Sprintf("plan: %s\nsources: %s\nresult: %s", s.plan, s.sources, s.resultNode)
}

type sourceRequest struct {
	source parser.Source
}

func (s sourceRequest) Process(ctx context.Context) error {
	return s.source.Execute(ctx)
}
