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

package aggregation

import (
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/parser"
	xsync "github.com/m3db/m3x/sync"
)

type baseNodeAsync struct {
	op         baseOp
	controller *transform.Controller

	workerPool xsync.PooledWorkerPool
}

// Node creates an execution node
func (o baseOp) AsyncNode(
	controller *transform.Controller,
	workerPool xsync.PooledWorkerPool,
) transform.OpNodeAsync {
	return &baseNodeAsync{
		op:         o,
		controller: controller,
		workerPool: workerPool,
	}
}

// ProcessSteps processes the block asynchronously using step channels.
func (n *baseNodeAsync) ProcessSteps(ID parser.NodeID, b block.AsyncBlock) error {
	stepIter, err := b.AsyncStepIter()
	if err != nil {
		return err
	}

	params := n.op.params
	meta := stepIter.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, metas := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		n.op.opType,
		seriesMetas,
	)
	meta.Tags, metas = utils.DedupeMetadata(metas)

	builder, err := n.controller.AsyncBlockBuilder(meta, metas)
	if err != nil {
		return err
	}

	stepCount := stepIter.StepCount()
	builder.AddCols(stepCount)
	if err := builder.Error(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(stepCount)
	for index := 0; stepIter.Next(); index++ {
		// capture loop variables
		index := index
		ch := stepIter.Current()
		n.workerPool.Go(func() {
			step := <-ch
			aggregatedValues := make([]float64, len(buckets))
			values := step.Values()
			for i, bucket := range buckets {
				aggregatedValues[i] = n.op.aggFn(values, bucket)
			}

			builder.AppendValues(index, aggregatedValues)
			wg.Done()
		})
	}

	wg.Wait()
	if err := builder.Error(); err != nil {
		return err
	}

	if err := stepIter.Err(); err != nil {
		return err
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return n.controller.Process(nextBlock)
}

// ProcessValueChannel processes the block asynchronously using the value channel.
func (n *baseNodeAsync) ProcessValueChannel(ID parser.NodeID, b block.AsyncBlock) error {
	stepIter, err := b.AsyncStepIter()
	if err != nil {
		return err
	}

	params := n.op.params
	meta := stepIter.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, metas := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		n.op.opType,
		seriesMetas,
	)
	meta.Tags, metas = utils.DedupeMetadata(metas)

	builder, err := n.controller.AsyncBlockBuilder(meta, metas)
	if err != nil {
		return err
	}

	stepCount := stepIter.StepCount()
	builder.AddCols(stepCount)
	if err := builder.Error(); err != nil {
		return err
	}

	aggregatedValues := make([]float64, len(buckets))
	for indexedStep := range stepIter.ValuesChannel() {
		values := indexedStep.Values()
		for i, bucket := range buckets {
			aggregatedValues[i] = n.op.aggFn(values, bucket)
		}

		builder.AppendValues(indexedStep.Idx, aggregatedValues)
	}

	if err := builder.Error(); err != nil {
		return err
	}

	if err := stepIter.Err(); err != nil {
		return err
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return n.controller.Process(nextBlock)
}
