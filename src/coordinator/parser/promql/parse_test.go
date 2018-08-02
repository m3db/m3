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
	"github.com/m3db/m3db/src/coordinator/functions/datetime"
	"github.com/m3db/m3db/src/coordinator/functions/linear"
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

func TestDAGWithAbsOp(t *testing.T) {
	q := "abs(http_requests_total{method=\"GET\"})"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), linear.AbsType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "function expr should be the child")
}

func TestDAGWithAbsentOp(t *testing.T) {
	q := "absent(http_requests_total{method=\"GET\"})"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), linear.AbsentType)
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

func TestDAGWithClampOp(t *testing.T) {
	q := "clamp_min(up, 1)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), linear.ClampMinType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "clamp op should be child")
}
func TestDAGWithLogOp(t *testing.T) {
	q := "ln(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), linear.LnType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "log op should be child")
}

func TestDAGWithCeilOp(t *testing.T) {
	q := "ceil(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.CeilType)
}

func TestDAGWithFloorOp(t *testing.T) {
	q := "floor(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.FloorType)
}

func TestDAGWithExpOp(t *testing.T) {
	q := "exp(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.ExpType)
}

func TestDAGWithSqrtOp(t *testing.T) {
	q := "sqrt(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.SqrtType)
}

func TestDAGWithLog2Op(t *testing.T) {
	q := "log2(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.Log2Type)
}

func TestDAGWithLog10Op(t *testing.T) {
	q := "log10(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.Log10Type)
}

func TestDAGWithRoundOp(t *testing.T) {
	q := "round(up, 10)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), linear.RoundType)
}

func TestDAGWithDayOfMonthOp(t *testing.T) {
	q := "day_of_month(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.DayOfMonthType)
}

func TestDAGWithDayOfWeekOp(t *testing.T) {
	q := "day_of_week(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.DayOfWeekType)
}
func TestDAGWithDaysInMonthOp(t *testing.T) {
	q := "days_in_month(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.DaysInMonthType)
}
func TestDAGWithHourOp(t *testing.T) {
	q := "hour(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.HourType)
}
func TestDAGWithMinuteOp(t *testing.T) {
	q := "minute(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.MinuteType)
}
func TestDAGWithMonthOp(t *testing.T) {
	q := "month(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.MonthType)
}
func TestDAGWithYearOp(t *testing.T) {
	q := "year(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.YearType)
}
func TestDAGWithTimestampOp(t *testing.T) {
	q := "timestamp(up)"
	p, err := Parse(q)
	require.NoError(t, err)
	transforms, _, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[1].Op.OpType(), datetime.TimestampType)
}
