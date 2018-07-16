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

package promql

import (
	"testing"

	"github.com/m3db/m3db/src/coordinator/functions"
	"github.com/m3db/m3db/src/coordinator/functions/logical"
	"github.com/m3db/m3db/src/coordinator/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAGWithCountOp(t *testing.T) {
	q := "count(http_requests_total{method=\"GET\"} offset 5m) by (service)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.CountType)
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "aggregation should be the child")

}

func TestDAGWithEmptyExpression(t *testing.T) {
	q := ""
	_, err := Parse(q)
	require.Error(t, err)
}

func TestDAGWithUnknownOp(t *testing.T) {
	q := "sum(http_requests_total{method=\"GET\"})"
	p, err := Parse(q)
	require.NoError(t, err)
	_, _, err = p.DAG()
	require.Error(t, err, "unsupported operation fails parsing")
}

func TestDAGWithFunctionCall(t *testing.T) {
	q := "abs(http_requests_total{method=\"GET\"})"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.AbsType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "function expr should be the child")
}

func TestDAGWithAndOp(t *testing.T) {
	q := "up and up"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 3)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[2].Op.OpType(), logical.AndType)
	assert.Equal(t, transforms[2].ID, parser.NodeID("2"))
	assert.Len(t, edges, 2)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("2"), "and op should be child")
	assert.Equal(t, edges[1].ParentID, parser.NodeID("1"), "second fetch should be the parent")
	assert.Equal(t, edges[1].ChildID, parser.NodeID("2"), "and op should be child")
}

func TestDAGWithOrOp(t *testing.T) {
	q := "up or up"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 3)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[2].Op.OpType(), logical.OrType)
	assert.Equal(t, transforms[2].ID, parser.NodeID("2"))
	assert.Len(t, edges, 2)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("2"), "or op should be child")
	assert.Equal(t, edges[1].ParentID, parser.NodeID("1"), "second fetch should be the parent")
	assert.Equal(t, edges[1].ChildID, parser.NodeID("2"), "or op should be child")
}

func TestDAGWithUnlessOp(t *testing.T) {
	q := "up unless up"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 3)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[2].Op.OpType(), logical.UnlessType)
	assert.Equal(t, transforms[2].ID, parser.NodeID("2"))
	assert.Len(t, edges, 2)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("2"), "or op should be child")
	assert.Equal(t, edges[1].ParentID, parser.NodeID("1"), "second fetch should be the parent")
	assert.Equal(t, edges[1].ChildID, parser.NodeID("2"), "or op should be child")
}

func TestDAGWithClampOp(t *testing.T) {
	q := "clamp_min(up, 1)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), functions.ClampMinType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "clamp op should be child")
}
