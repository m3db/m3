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

package scalar

import (
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/test/transformtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScalar(t *testing.T) {
	val := 10.0
	_, bounds := test.GenerateValuesAndBounds(nil, nil)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(0)))
	op, err := NewScalarOp(val, models.NewTagOptions())
	require.NoError(t, err)

	baseOp, ok := op.(*ScalarOp)
	require.True(t, ok)
	start := bounds.Start
	step := bounds.StepSize
	node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: start,
			End:   bounds.End(),
			Step:  step,
		},
	}))

	err = node.Execute(models.NoopQueryContext())
	require.NoError(t, err)
	require.Equal(t, 1, len(sink.Values))
	require.Equal(t, block.BlockScalar, sink.Info.BaseType())

	vals := sink.Values[0]
	assert.Equal(t, bounds.Steps(), len(vals))
	for _, v := range vals {
		assert.Equal(t, val, v)
	}

	resultMeta := sink.Meta.ResultMetadata
	require.True(t, resultMeta.Exhaustive)
	require.True(t, resultMeta.LocalOnly)
	require.Equal(t, 0, len(resultMeta.Warnings))
}
