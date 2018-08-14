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

	"github.com/m3db/m3/src/query/functions"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/functions/linear"
	"github.com/m3db/m3/src/query/functions/logical"
	"github.com/m3db/m3/src/query/parser"

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

var linearParseTests = []struct {
	q            string
	expectedType string
}{
	{"abs(up)", linear.AbsType},
	{"absent(up)", linear.AbsentType},
	{"ceil(up)", linear.CeilType},
	{"clamp_min(up, 1)", linear.ClampMinType},
	{"clamp_max(up, 1)", linear.ClampMaxType},
	{"exp(up)", linear.ExpType},
	{"floor(up)", linear.FloorType},
	{"ln(up)", linear.LnType},
	{"log2(up)", linear.Log2Type},
	{"log10(up)", linear.Log10Type},
	{"sqrt(up)", linear.SqrtType},
	{"round(up, 10)", linear.RoundType},
	{"day_of_month(up)", linear.DayOfMonthType},
	{"day_of_week(up)", linear.DayOfWeekType},
	{"day_of_month(up)", linear.DayOfMonthType},
	{"days_in_month(up)", linear.DaysInMonthType},

	{"hour(up)", linear.HourType},
	{"minute(up)", linear.MinuteType},
	{"month(up)", linear.MonthType},
	{"year(up)", linear.YearType},
}

func TestLinearParses(t *testing.T) {
	for _, tt := range linearParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q)
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			assert.Len(t, transforms, 2)
			assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
			assert.Equal(t, transforms[1].Op.OpType(), tt.expectedType)
			assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
			assert.Len(t, edges, 1)
			assert.Equal(t, edges[0].ParentID, parser.NodeID("0"))
			assert.Equal(t, edges[0].ChildID, parser.NodeID("1"))
		})
	}
}

var binaryParseTests = []struct {
	q                string
	LHSType, RHSType string
	expectedType     string
}{
	// Arithmetic
	{"up / up", functions.FetchType, functions.FetchType, binary.DivType},
	{"up ^ 10", functions.FetchType, functions.ScalarType, binary.ExpType},
	{"10 - up", functions.ScalarType, functions.FetchType, binary.MinusType},
	{"10 + 10", functions.ScalarType, functions.ScalarType, binary.PlusType},
	{"up % up", functions.FetchType, functions.FetchType, binary.ModType},
	{"up * 10", functions.FetchType, functions.ScalarType, binary.MultiplyType},

	// Equality
	{"up == up", functions.FetchType, functions.FetchType, binary.EqType},
	{"up != 10", functions.FetchType, functions.ScalarType, binary.NotEqType},
	{"up > up", functions.FetchType, functions.FetchType, binary.GreaterType},
	{"10 < up", functions.ScalarType, functions.FetchType, binary.LesserType},
	{"up >= 10", functions.FetchType, functions.ScalarType, binary.GreaterEqType},
	{"up <= 10", functions.FetchType, functions.ScalarType, binary.LesserEqType},

	// Logical
	{"up and up", functions.FetchType, functions.FetchType, logical.AndType},
	{"up or up", functions.FetchType, functions.FetchType, logical.OrType},
	{"up unless up", functions.FetchType, functions.FetchType, logical.UnlessType},
}

func TestBinaryParses(t *testing.T) {
	for _, tt := range binaryParseTests {
		t.Run(tt.q, func(t *testing.T) {
			p, err := Parse(tt.q)
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			require.Len(t, transforms, 3)
			assert.Equal(t, transforms[0].Op.OpType(), tt.LHSType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
			assert.Equal(t, transforms[1].Op.OpType(), tt.RHSType)
			assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
			assert.Equal(t, transforms[2].Op.OpType(), tt.expectedType)
			assert.Equal(t, transforms[2].ID, parser.NodeID("2"))
			assert.Len(t, edges, 2)
			assert.Equal(t, edges[0].ParentID, parser.NodeID("0"))
			assert.Equal(t, edges[0].ChildID, parser.NodeID("2"))
			assert.Equal(t, edges[1].ParentID, parser.NodeID("1"))
			assert.Equal(t, edges[1].ChildID, parser.NodeID("2"))
		})
	}
}
