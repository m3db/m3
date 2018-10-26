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
	"context"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScalarTime(t *testing.T) {
	_, bounds := test.GenerateValuesAndBounds(nil, nil)
	c, sink := executor.NewControllerWithSink(parser.NodeID(0))
	baseOp := baseOp{
		fn:           func(t time.Time) float64 { return float64(t.Unix()) },
		operatorType: TimeType,
	}

	start := bounds.Start
	step := bounds.StepSize
	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: start,
			End:   bounds.End(),
			Step:  step,
		},
	})
	err := node.Execute(context.Background(), models.NoopQueryContext())
	require.NoError(t, err)
	assert.Len(t, sink.Values, 1)

	for _, vals := range sink.Values {
		for i, val := range vals {
			assert.Equal(t, float64(start.Add(time.Duration(i)*step).Unix()), val)
		}
	}
}
