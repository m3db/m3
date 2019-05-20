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

package lazy

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildMeta(start time.Time) block.Metadata {
	return block.Metadata{
		Bounds: models.Bounds{
			Start:    start,
			Duration: time.Minute,
			StepSize: time.Hour,
		},
		Tags: models.NewTags(0, models.NewTagOptions()),
	}
}

func testLazyOpts(timeOffset time.Duration, valOffset float64) block.LazyOptions {
	tt := func(t time.Time) time.Time { return t.Add(timeOffset) }
	mt := func(meta block.Metadata) block.Metadata {
		meta.Bounds.Start = meta.Bounds.Start.Add(timeOffset)
		return meta
	}
	vt := func(val float64) float64 { return val * valOffset }

	return block.NewLazyOpts().SetTimeTransform(tt).SetMetaTransform(mt).SetValueTransform(vt)
}

func TestOffsetOp(t *testing.T) {
	offset := time.Minute
	op, err := NewLazyOp(OffsetType, testLazyOpts(offset, 1.0))
	assert.NoError(t, err)

	assert.Equal(t, "offset", op.OpType())
	assert.Equal(t, "type: offset", op.String())

	base, ok := op.(baseOp)
	require.True(t, ok)

	node := base.Node(nil, transform.Options{})
	n, ok := node.(*baseNode)
	require.True(t, ok)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := block.NewMockBlock(ctrl)

	bl := n.processBlock(b)
	it := block.NewMockStepIter(ctrl)
	b.EXPECT().StepIter().Return(it, nil)

	iter, err := bl.StepIter()
	require.NoError(t, err)

	vals := []float64{1, 2, 3, 4}
	now := time.Now()

	step := block.NewMockStep(ctrl)
	step.EXPECT().Time().Return(now)
	step.EXPECT().Values().Return(vals)
	it.EXPECT().Current().Return(step)
	actual := iter.Current()

	assert.Equal(t, vals, actual.Values())
	assert.Equal(t, now.Add(offset), actual.Time())
}

func TestUnaryOp(t *testing.T) {
	offset := time.Duration(0)
	op, err := NewLazyOp(UnaryType, testLazyOpts(offset, -1.0))
	assert.NoError(t, err)

	assert.Equal(t, "unary", op.OpType())
	assert.Equal(t, "type: unary", op.String())

	base, ok := op.(baseOp)
	require.True(t, ok)

	node := base.Node(nil, transform.Options{})
	n, ok := node.(*baseNode)
	require.True(t, ok)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := block.NewMockBlock(ctrl)

	bl := n.processBlock(b)
	it := block.NewMockStepIter(ctrl)
	b.EXPECT().StepIter().Return(it, nil)

	iter, err := bl.StepIter()
	require.NoError(t, err)

	vals := []float64{1, 2, 3, 4}
	now := time.Now()

	step := block.NewMockStep(ctrl)
	step.EXPECT().Time().Return(now)
	step.EXPECT().Values().Return(vals)
	it.EXPECT().Current().Return(step)
	actual := iter.Current()

	expectedVals := []float64{-1, -2, -3, -4}
	assert.Equal(t, expectedVals, actual.Values())
	assert.Equal(t, now, actual.Time())
}
