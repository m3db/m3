// Copyright (c) 2019 Uber Technologies, Inc.
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

package transform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessSimpleBlock(t *testing.T) {
	type testContext struct {
		MockCtrl    *gomock.Controller
		Controller  *Controller
		ChildNode   *MockOpNode
		Node        *MocksimpleOpNode
		ResultBlock *block.MockBlock
		SourceBlock block.Block
		QueryCtx    *models.QueryContext
	}

	setup := func(t *testing.T) (*testContext, func()) {
		ctrl := xtest.NewController(t)

		controller := &Controller{
			ID: parser.NodeID("foo"),
		}
		child := NewMockOpNode(ctrl)
		controller.AddTransform(child)

		step := time.Second
		bounds := models.Bounds{
			StepSize: step,
			Duration: step,
		}

		return &testContext{
			MockCtrl:    ctrl,
			Controller:  controller,
			SourceBlock: test.NewBlockFromValues(bounds, [][]float64{{1.0}}),
			ResultBlock: block.NewMockBlock(ctrl),
			Node:        NewMocksimpleOpNode(ctrl),
			ChildNode:   child,
			QueryCtx:    models.NoopQueryContext(),
		}, ctrl.Finish
	}

	doCall := func(tctx *testContext) error {
		return ProcessSimpleBlock(tctx.Node, tctx.Controller, tctx.QueryCtx, tctx.Controller.ID, tctx.SourceBlock)
	}

	configureNode := func(
		tctx *testContext,
		blockType block.BlockType,
		closeExpected bool,
	) {
		tctx.Node.EXPECT().Params().Return(utils.StaticParams("foo"))
		tctx.Node.EXPECT().ProcessBlock(gomock.Any(), gomock.Any(), gomock.Any()).Return(tctx.ResultBlock, nil)
		tctx.ChildNode.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any())
		tctx.ResultBlock.EXPECT().Info().Return(block.NewBlockInfo(blockType))
		if closeExpected {
			tctx.ResultBlock.EXPECT().Close()
		}
	}

	configureSuccessfulNode := func(tctx *testContext) {
		configureNode(tctx, block.BlockM3TSZCompressed, true)
	}

	t.Run("closes next block", func(t *testing.T) {
		tctx, closer := setup(t)
		defer closer()

		configureSuccessfulNode(tctx)

		require.NoError(t, doCall(tctx))
	})

	configureLazyNode := func(tctx *testContext) {
		configureNode(tctx, block.BlockLazy, false)
	}

	t.Run("does not close lazy block", func(t *testing.T) {
		tctx, closer := setup(t)
		defer closer()

		configureLazyNode(tctx)

		require.NoError(t, doCall(tctx))
	})

	t.Run("errors on process error", func(t *testing.T) {
		tctx, closer := setup(t)
		defer closer()

		expectedErr := errors.New("test err")
		tctx.Node.EXPECT().Params().Return(utils.StaticParams("foo"))
		tctx.Node.EXPECT().ProcessBlock(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, expectedErr)

		require.EqualError(t, doCall(tctx), expectedErr.Error())
	})

	t.Run("starts span with op type", func(t *testing.T) {
		tctx, closer := setup(t)
		defer closer()

		configureSuccessfulNode(tctx)
		tctx.Node.EXPECT().Params().Return(utils.StaticParams("foo"))

		mtr := mocktracer.New()

		sp := mtr.StartSpan("root")
		tctx.QueryCtx.Ctx = opentracing.ContextWithSpan(context.Background(), sp)

		require.NoError(t, doCall(tctx))
		sp.Finish()

		spans := mtr.FinishedSpans()

		require.Len(t, spans, 2)
		assert.Equal(t, tctx.Node.Params().OpType(), spans[0].OperationName)
	})
}
