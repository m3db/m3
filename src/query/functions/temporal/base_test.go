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

package temporal

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/test/transformtest"
	"github.com/m3db/m3/src/query/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopProcessor struct{}

func (p noopProcessor) initialize(
	_ time.Duration,
	controller *transform.Controller,
	opts transform.Options,
) processor {
	return &p
}

func (p *noopProcessor) process(dps ts.Datapoints, _ iterationBounds) float64 {
	vals := dps.Values()
	sum := 0.0
	for _, n := range vals {
		sum += n
	}

	return sum
}

func compareCacheState(t *testing.T, node *baseNode,
	bounds models.Bounds, state []bool, debugMsg string) {
	actualState := make([]bool, len(state))
	for i := range state {
		_, exists := node.cache.get(bounds.Next(i).Start)
		actualState[i] = exists
	}

	assert.Equal(t, state, actualState, debugMsg)
}

func TestBaseWithB0(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block := test.NewUnconsolidatedBlockFromDatapoints(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  noopProcessor{},
	}

	node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: boundStart,
			End:   boundStart.Add(time.Hour),
			Step:  time.Second,
		},
	}))
	err := node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2)
	require.IsType(t, node, &baseNode{})
	bNode := node.(*baseNode)
	_, exists := bNode.cache.get(boundStart)
	assert.True(t, exists, "block cached since the query end is larger")

	c, _ = executor.NewControllerWithSink(parser.NodeID(1))
	node = baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: boundStart,
			End:   bounds.End(),
			Step:  time.Second,
		},
	}))

	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	bNode = node.(*baseNode)
	_, exists = bNode.cache.get(boundStart)
	assert.False(t, exists, "block not cached since no other blocks left to process")

	c, _ = executor.NewControllerWithSink(parser.NodeID(1))
	node = baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(bounds.StepSize),
			End:   bounds.End().Add(-1 * bounds.StepSize),
			Step:  time.Second,
		},
	}))

	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	bNode = node.(*baseNode)
	_, exists = bNode.cache.get(boundStart)
	assert.False(t, exists, "block not cached since no other blocks left to process")
}

func TestBaseWithB1B0(t *testing.T) {
	tc := setup(t, 2, 5*time.Minute, 1)

	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "nothing processed yet")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true}, "B1 cached")

	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from both blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false}, "everything removed from cache")
	blks, err := tc.Node.cache.multiGet(tc.Bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1(t *testing.T) {
	tc := setup(t, 2, 5*time.Minute, 1)

	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false}, "B0 cached for future")

	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from both blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false}, "B0 removed from cache, B1 not cached")
	blks, err := tc.Node.cache.multiGet(tc.Bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1B2(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false}, "B0 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from B0, B1")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B0 removed from cache, B1 cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB0B2B1(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false}, "B0 cached for future")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "Only B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B0B2(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B1 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached for future")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 still cached, B0 not cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B2B0(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B1 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached for future")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 still cached, B2 not cached")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B0B1(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B2 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, true}, "B2 cached for future")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B1B0(t *testing.T) {
	tc := setup(t, 3, 5*time.Minute, 2)

	// B2 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, true}, "B2 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached, B2 removed")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithSize3B0B1B2B3B4(t *testing.T) {
	tc := setup(t, 5, 15*time.Minute, 4)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false, false, false}, "B0 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, true, false, false, false}, "B0, B1 cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "B0, B1, B2 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, true, true, false, false}, "B0, B1, B2 cached")

	// B3 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[3])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 8, "B0, B1, B2, B3 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, true, true, false}, "B0 removed, B1, B2, B3 cached")

	// B4 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[4])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 10, "all 5 blocks processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false, false, false}, "nothing cached")
}

type testContext struct {
	Bounds models.Bounds
	Blocks []block.Block
	Sink   *executor.SinkNode
	Node   *baseNode
}

func setup(
	t *testing.T,
	numBlocks int,
	duration time.Duration,
	nextBound int,
) *testContext {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, numBlocks)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     duration,
		processorFn:  noopProcessor{},
	}
	node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(nextBound).End(),
			Step:  time.Second,
		},
	}))
	return &testContext{
		Bounds: bounds,
		Blocks: blocks,
		Sink:   sink,
		Node:   node.(*baseNode),
	}
}

// TestBaseWithDownstreamError checks that we handle errors from blocks correctly
func TestBaseWithDownstreamError(t *testing.T) {
	numBlocks := 2
	tc := setup(t, numBlocks, 5*time.Minute, 1)

	testErr := errors.New("test err")
	errBlock := blockWithDownstreamErr{Block: tc.Blocks[1], Err: testErr}

	require.NoError(t, tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), errBlock))

	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.EqualError(t, err, testErr.Error())
}

// Types for TestBaseWithDownstreamError

// blockWithDownstreamErr overrides only Unconsolidated() for purposes of returning a
// an UnconsolidatedBlock which errors on SeriesIter() (unconsolidatedBlockWithSeriesIterErr)
type blockWithDownstreamErr struct {
	block.Block
	Err error
}

func (mbu blockWithDownstreamErr) Unconsolidated() (block.UnconsolidatedBlock, error) {
	unconsolidated, err := mbu.Block.Unconsolidated()
	if err != nil {
		return nil, err
	}
	return unconsolidatedBlockWithSeriesIterErr{
		Err:                 mbu.Err,
		UnconsolidatedBlock: unconsolidated,
	}, nil
}

type unconsolidatedBlockWithSeriesIterErr struct {
	block.UnconsolidatedBlock
	Err error
}

func (mbuc unconsolidatedBlockWithSeriesIterErr) SeriesIter() (block.UnconsolidatedSeriesIter, error) {
	return nil, mbuc.Err
}

// End types for TestBaseWithDownstreamError

func TestBaseClosesBlocks(t *testing.T) {
	tc := setup(t, 1, 5*time.Minute, 1)

	ctrl := gomock.NewController(t)
	builderCtx := setupCloseableBlock(ctrl, tc.Node)

	require.NoError(t, tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0]))

	for _, mockBuilder := range builderCtx.MockBlockBuilders {
		assert.Equal(t, 1, mockBuilder.BuiltBlock.ClosedCalls)
	}
}

func TestProcessCompletedBlocks_ClosesBlocksOnError(t *testing.T) {
	numBlocks := 2
	tc := setup(t, numBlocks, 5*time.Minute, 1)
	ctrl := gomock.NewController(t)
	setupCloseableBlock(ctrl, tc.Node)

	testErr := errors.New("test err")
	tc.Blocks[1] = blockWithDownstreamErr{Block: tc.Blocks[1], Err: testErr}

	processRequests := make([]processRequest, numBlocks)
	for i, blk := range tc.Blocks {
		unconsolidated, err := blk.Unconsolidated()
		require.NoError(t, err)

		processRequests[i] = processRequest{
			queryCtx: models.NoopQueryContext(),
			blk:      unconsolidated,
			bounds:   tc.Bounds,
			deps:     nil,
		}
	}

	blocks, err := tc.Node.processCompletedBlocks(models.NoopQueryContext(), processRequests, numBlocks)
	require.EqualError(t, err, testErr.Error())

	for _, bl := range blocks {
		require.NotNil(t, bl)
		assert.Equal(t, 1, bl.(*closeSpyBlock).ClosedCalls)
	}
}

type closeableBlockBuilderContext struct {
	MockController    *Mockcontroller
	MockBlockBuilders []*closeSpyBlockBuilder
}

// setupCloseableBlock mocks out node.controller to return a block builder which
// builds closeSpyBlock instances, so that you can inspect whether
// or not a block was closed (using closeSpyBlock.ClosedCalls). See TestBaseClosesBlocks
// for an example.
func setupCloseableBlock(ctrl *gomock.Controller, node *baseNode) closeableBlockBuilderContext {
	mockController := NewMockcontroller(ctrl)
	mockBuilders := make([]*closeSpyBlockBuilder, 0)

	mockController.EXPECT().Process(gomock.Any(), gomock.Any()).Return(nil)

	// return a regular ColumnBlockBuilder, wrapped with closeSpyBlockBuilder
	mockController.EXPECT().BlockBuilder(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			queryCtx *models.QueryContext,
			blockMeta block.Metadata,
			seriesMeta []block.SeriesMeta) (block.Builder, error) {
			mb := &closeSpyBlockBuilder{
				Builder: block.NewColumnBlockBuilder(models.NoopQueryContext(), blockMeta, seriesMeta),
			}
			mockBuilders = append(mockBuilders, mb)
			return mb, nil
		})

	node.controller = mockController

	return closeableBlockBuilderContext{
		MockController:    mockController,
		MockBlockBuilders: mockBuilders,
	}
}

// closeSpyBlockBuilder wraps a block.Builder to build a closeSpyBlock
// instead of a regular block. It is otherwise equivalent to the wrapped Builder.
type closeSpyBlockBuilder struct {
	block.Builder

	BuiltBlock *closeSpyBlock
}

func (bb *closeSpyBlockBuilder) Build() block.Block {
	bb.BuiltBlock = &closeSpyBlock{
		Block: bb.Builder.Build(),
	}
	return bb.BuiltBlock
}

// closeSpyBlock wraps a block.Block to allow assertions on the Close()
// method.
type closeSpyBlock struct {
	block.Block

	ClosedCalls int
}

func (b *closeSpyBlock) Close() error {
	b.ClosedCalls++
	return nil
}

func TestSingleProcessRequest(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	bounds.Start = bounds.Start.Truncate(time.Hour)
	boundStart := bounds.Start

	seriesMetas := []block.SeriesMeta{{
		Name: []byte("s1"),
		Tags: models.EmptyTags().AddTags([]models.Tag{{
			Name:  []byte("t1"),
			Value: []byte("v1"),
		}})}, {
		Name: []byte("s2"),
		Tags: models.EmptyTags().AddTags([]models.Tag{{
			Name:  []byte("t1"),
			Value: []byte("v2"),
		}}),
	}}

	b := test.NewUnconsolidatedBlockFromDatapointsWithMeta(bounds, seriesMetas, values)
	block2, _ := b.Unconsolidated()
	values = [][]float64{{10, 11, 12, 13, 14}, {15, 16, 17, 18, 19}}

	block1Bounds := models.Bounds{
		Start:    bounds.Start.Add(-1 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}

	b = test.NewUnconsolidatedBlockFromDatapointsWithMeta(block1Bounds, seriesMetas, values)
	block1, _ := b.Unconsolidated()

	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  noopProcessor{},
	}

	node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(-2 * bounds.Duration),
			End:   bounds.End(),
			Step:  time.Second,
		},
	}))
	bNode := node.(*baseNode)
	request := processRequest{
		blk:      block2,
		bounds:   bounds,
		deps:     []block.UnconsolidatedBlock{block1},
		queryCtx: models.NoopQueryContext(),
	}

	bl, err := bNode.processSingleRequest(request, nil)
	require.NoError(t, err)

	bNode.propagateNextBlocks([]processRequest{request}, []block.Block{bl}, 1)
	assert.Len(t, sink.Values, 2, "block processed")
	/*
		NB: This test is a little weird to understand; it's simulating a test where
		blocks come in out of order. Worked example for expected values below. As
		a processing function, it simply adds all values together.

		For series 1:
			Previous Block:   10 11 12 13 14, with 10 being outside of the period.
			Current Block:     0  1  2  3  4

			1st value of processed block uses values: [11, 12, 13, 14], [0] = 50
			2nd value of processed block uses values: [12, 13, 14], [0, 1] = 40
			3rd value of processed block uses values: [13, 14], [0, 1, 2] = 30
			4th value of processed block uses values: [14], [0, 1, 2, 3] = 20
			5th value of processed block uses values: [0, 1, 2, 3, 4] = 10
	*/
	require.Equal(t, sink.Values[0], []float64{50, 40, 30, 20, 10})
	assert.Equal(t, sink.Values[1], []float64{75, 65, 55, 45, 35})

	// processSingleRequest renames the series to use their ids; reflect this in our expectation.
	expectedSeriesMetas := make([]block.SeriesMeta, len(seriesMetas))
	require.Equal(t, len(expectedSeriesMetas), copy(expectedSeriesMetas, seriesMetas))
	expectedSeriesMetas[0].Name = []byte("t1=v1,")
	expectedSeriesMetas[1].Name = []byte("t1=v2,")

	assert.Equal(t, expectedSeriesMetas, sink.Metas, "Process should pass along series meta, renaming to the ID")
}
