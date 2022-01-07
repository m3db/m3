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

package executor

import (
	"context"
	"fmt"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/plan"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/execution"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/pkg/errors"
)

// ExecutionState represents the execution hierarchy.
type ExecutionState struct {
	plan    plan.PhysicalPlan
	sources []parser.Source
	sink    sink
	storage storage.Storage
}

// CreateSource creates a source node.
func CreateSource(
	ID parser.NodeID,
	params SourceParams, storage storage.Storage,
	options transform.Options,
) (parser.Source, *transform.Controller) {
	controller := &transform.Controller{ID: ID}
	return params.Node(controller, storage, options), controller
}

// CreateScalarSource creates a scalar source node.
func CreateScalarSource(
	ID parser.NodeID,
	params ScalarParams,
	options transform.Options,
) (parser.Source, *transform.Controller) {
	controller := &transform.Controller{ID: ID}
	return params.Node(controller, options), controller
}

// CreateTransform creates a transform node which works on functions and
// contains state.
func CreateTransform(
	ID parser.NodeID,
	params transform.Params,
	options transform.Options,
) (transform.OpNode, *transform.Controller) {
	controller := &transform.Controller{ID: ID}
	return params.Node(controller, options), controller
}

// SourceParams are defined by sources.
type SourceParams interface {
	parser.Params
	Node(ctrl *transform.Controller, storage storage.Storage,
		opts transform.Options) parser.Source
}

// ScalarParams are defined by sources.
type ScalarParams interface {
	parser.Params
	Node(ctrl *transform.Controller, opts transform.Options) parser.Source
}

// GenerateExecutionState creates an execution state from the physical plan.
func GenerateExecutionState(
	pplan plan.PhysicalPlan,
	storage storage.Storage,
	fetchOpts *storage.FetchOptions,
	instrumentOpts instrument.Options,
) (*ExecutionState, error) {
	result := pplan.ResultStep
	state := &ExecutionState{
		plan:    pplan,
		storage: storage,
	}

	step, ok := pplan.Step(result.Parent)
	if !ok {
		return nil, fmt.Errorf("incorrect parent reference in result node, "+
			"parentId: %s", result.Parent)
	}
	fmt.Printf("transform: %v \n", step.Transform)

	options, err := transform.NewOptions(transform.OptionsParams{
		FetchOptions:      fetchOpts,
		TimeSpec:          pplan.TimeSpec,
		Debug:             pplan.Debug,
		BlockType:         pplan.BlockType,
		InstrumentOptions: instrumentOpts,
	})
	if err != nil {
		return nil, err
	}

	controller, err := state.createNode(step, options)
	if err != nil {
		return nil, err
	}

	fmt.Printf("sources: %v \n", state.sources[0])
	if len(state.sources) == 0 {
		return nil, errors.New("empty sources for the execution state")
	}

	sink := newResultNode()
	state.sink = sink
	controller.AddTransform(sink)

	return state, nil
}

// createNode helps to create an execution node recursively.
func (s *ExecutionState) createNode(
	step plan.LogicalStep,
	options transform.Options,
) (*transform.Controller, error) {
	// TODO: consider using a registry instead of casting to an interface.
	sourceParams, ok := step.Transform.Op.(SourceParams)
	if ok {
		fmt.Printf("srouceParams: %s \n", sourceParams.String())
		source, controller := CreateSource(step.ID(), sourceParams,
			s.storage, options)
		s.sources = append(s.sources, source)
		return controller, nil
	}

	scalarParams, ok := step.Transform.Op.(ScalarParams)
	if ok {
		source, controller := CreateScalarSource(step.ID(), scalarParams, options)
		s.sources = append(s.sources, source)
		return controller, nil
	}

	transformParams, ok := step.Transform.Op.(transform.Params)
	if !ok {
		return nil, fmt.Errorf("invalid transform step: %s", step)
	}

	transformNode, controller := CreateTransform(step.ID(),
		transformParams, options)
	for _, parentID := range step.Parents {
		parentStep, ok := s.plan.Step(parentID)
		if !ok {
			return nil, fmt.Errorf("incorrect parent reference, parentId: "+
				"%s, node: %s", parentID, step.ID())
		}

		parentController, err := s.createNode(parentStep, options)
		if err != nil {
			return nil, err
		}

		parentController.AddTransform(transformNode)
	}

	return controller, nil
}

// Execute the sources in parallel and return the first error.
func (s *ExecutionState) Execute(queryCtx *models.QueryContext) error {
	requests := make([]execution.Request, 0, len(s.sources))
	for _, source := range s.sources {
		fmt.Printf("executing state %v \n", source)
		requests = append(requests, sourceRequest{
			source:   source,
			queryCtx: queryCtx,
		})
	}

	return execution.ExecuteParallel(queryCtx.Ctx, requests)
}

// String representation of the state.
func (s *ExecutionState) String() string {
	return fmt.Sprintf("plan: %s\nsources: %s\n", s.plan, s.sources)
}

type sourceRequest struct {
	source   parser.Source
	queryCtx *models.QueryContext
}

// Process processes the new request.
func (s sourceRequest) Process(ctx context.Context) error {
	// make sure to propagate the new context.Context object down.
	return s.source.Execute(s.queryCtx.WithContext(ctx))
}
