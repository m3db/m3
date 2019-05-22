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
	"github.com/m3db/m3/src/query/functions/aggregation"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/functions/linear"
	"github.com/m3db/m3/src/query/functions/scalar"
	"github.com/m3db/m3/src/query/functions/tag"
	"github.com/m3db/m3/src/query/functions/temporal"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/prometheus/prometheus/promql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAGWithCountOp(t *testing.T) {
	q := "count(http_requests_total{method=\"GET\"}) by (service)"
	p, err := Parse(q, models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[1].Op.OpType(), aggregation.CountType)
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "aggregation should be the child")
}

func TestDAGWithOffset(t *testing.T) {
	q := "up offset 2m"
	p, err := Parse(q, models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[1].Op.OpType(), lazy.OffsetType)
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"), "fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"), "offset should be the child")
}

func TestInvalidOffset(t *testing.T) {
	q := "up offset -2m"
	_, err := Parse(q, models.NewTagOptions())
	require.Error(t, err)
}

func TestUnary(t *testing.T) {
	q := "-up"
	p, err := Parse(q, models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].Op.OpType(), lazy.UnaryType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"))
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"))
}

func TestInvalidUnary(t *testing.T) {
	q := "*up"
	_, err := Parse(q, models.NewTagOptions())
	require.Error(t, err)
}

func TestGetUnaryOpType(t *testing.T) {
	promOpType := promql.ItemType(itemADD)
	unaryOpType, err := getUnaryOpType(promOpType)
	require.NoError(t, err)
	assert.Equal(t, binary.PlusType, unaryOpType)

	promOpType = promql.ItemType(itemEQL)
	_, err = getUnaryOpType(promOpType)
	require.Error(t, err)
}

func TestDAGWithEmptyExpression(t *testing.T) {
	q := ""
	_, err := Parse(q, models.NewTagOptions())
	require.Error(t, err)
}

func TestDAGWithFakeOp(t *testing.T) {
	q := "fake(http_requests_total{method=\"GET\"})"
	_, err := Parse(q, models.NewTagOptions())
	require.Error(t, err)
}

var aggregateParseTests = []struct {
	q            string
	expectedType string
}{
	{"sum(up)", aggregation.SumType},
	{"min(up)", aggregation.MinType},
	{"max(up)", aggregation.MaxType},
	{"avg(up)", aggregation.AverageType},
	{"stddev(up)", aggregation.StandardDeviationType},
	{"stdvar(up)", aggregation.StandardVarianceType},
	{"count(up)", aggregation.CountType},

	{"topk(3, up)", aggregation.TopKType},
	{"bottomk(3, up)", aggregation.BottomKType},
	{"quantile(3, up)", aggregation.QuantileType},
	{"count_values(\"some_name\", up)", aggregation.CountValuesType},
}

func TestAggregateParses(t *testing.T) {
	for _, tt := range aggregateParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, models.NewTagOptions())
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

	{"histogram_quantile(1,up)", linear.HistogramQuantileType},
}

func TestLinearParses(t *testing.T) {
	for _, tt := range linearParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, models.NewTagOptions())
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

var sortTests = []struct {
	q            string
	expectedType string
}{
	{"sort(up)", linear.SortType},
	{"sort_desc(up)", linear.SortDescType},
}

func TestSort(t *testing.T) {
	for _, tt := range sortTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, models.NewTagOptions())
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			assert.Len(t, transforms, 1)
			assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
			assert.Len(t, edges, 0)
		})
	}
}

func TestScalar(t *testing.T) {
	p, err := Parse("scalar(up)", models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 1)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Len(t, edges, 0)
}

func TestTimeTypeParse(t *testing.T) {
	q := "time()"
	p, err := Parse(q, models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 1)
	assert.Equal(t, transforms[0].Op.OpType(), scalar.TimeType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Len(t, edges, 0)
}

var binaryParseTests = []struct {
	q                string
	LHSType, RHSType string
	expectedType     string
}{
	// Arithmetic
	{"up / up", functions.FetchType, functions.FetchType, binary.DivType},
	{"up ^ 10", functions.FetchType, scalar.ScalarType, binary.ExpType},
	{"10 - up", scalar.ScalarType, functions.FetchType, binary.MinusType},
	{"10 + 10", scalar.ScalarType, scalar.ScalarType, binary.PlusType},
	{"up % up", functions.FetchType, functions.FetchType, binary.ModType},
	{"up * 10", functions.FetchType, scalar.ScalarType, binary.MultiplyType},

	// Equality
	{"up == up", functions.FetchType, functions.FetchType, binary.EqType},
	{"up != 10", functions.FetchType, scalar.ScalarType, binary.NotEqType},
	{"up > up", functions.FetchType, functions.FetchType, binary.GreaterType},
	{"10 < up", scalar.ScalarType, functions.FetchType, binary.LesserType},
	{"up >= 10", functions.FetchType, scalar.ScalarType, binary.GreaterEqType},
	{"up <= 10", functions.FetchType, scalar.ScalarType, binary.LesserEqType},

	// Logical
	{"up and up", functions.FetchType, functions.FetchType, binary.AndType},
	{"up or up", functions.FetchType, functions.FetchType, binary.OrType},
	{"up unless up", functions.FetchType, functions.FetchType, binary.UnlessType},
}

func TestBinaryParses(t *testing.T) {
	for _, tt := range binaryParseTests {
		t.Run(tt.q, func(t *testing.T) {
			p, err := Parse(tt.q, models.NewTagOptions())

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

func TestParenPrecedenceParses(t *testing.T) {
	p, err := Parse("(5^(up-6))", models.NewTagOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	require.Len(t, transforms, 5)
	// 5
	assert.Equal(t, transforms[0].Op.OpType(), scalar.ScalarType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	// up
	assert.Equal(t, transforms[1].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	// 6
	assert.Equal(t, transforms[2].Op.OpType(), scalar.ScalarType)
	assert.Equal(t, transforms[2].ID, parser.NodeID("2"))
	// -
	assert.Equal(t, transforms[3].Op.OpType(), binary.MinusType)
	assert.Equal(t, transforms[3].ID, parser.NodeID("3"))
	// ^
	assert.Equal(t, transforms[4].Op.OpType(), binary.ExpType)
	assert.Equal(t, transforms[4].ID, parser.NodeID("4"))

	assert.Len(t, edges, 4)
	// up -
	assert.Equal(t, edges[0].ParentID, parser.NodeID("1"))
	assert.Equal(t, edges[0].ChildID, parser.NodeID("3"))
	// 6 -
	assert.Equal(t, edges[1].ParentID, parser.NodeID("2"))
	assert.Equal(t, edges[1].ChildID, parser.NodeID("3"))
	// 5 ^
	assert.Equal(t, edges[2].ParentID, parser.NodeID("0"))
	assert.Equal(t, edges[2].ChildID, parser.NodeID("4"))
	// (up -6) ^
	assert.Equal(t, edges[3].ParentID, parser.NodeID("3"))
	assert.Equal(t, edges[3].ChildID, parser.NodeID("4"))
}

var temporalParseTests = []struct {
	q            string
	expectedType string
}{
	{"avg_over_time(up[5m])", temporal.AvgType},
	{"count_over_time(up[5m])", temporal.CountType},
	{"min_over_time(up[5m])", temporal.MinType},
	{"max_over_time(up[5m])", temporal.MaxType},
	{"sum_over_time(up[5m])", temporal.SumType},
	{"stddev_over_time(up[5m])", temporal.StdDevType},
	{"stdvar_over_time(up[5m])", temporal.StdVarType},
	{"quantile_over_time(0.2, up[5m])", temporal.QuantileType},
	{"irate(up[5m])", temporal.IRateType},
	{"idelta(up[5m])", temporal.IDeltaType},
	{"rate(up[5m])", temporal.RateType},
	{"delta(up[5m])", temporal.DeltaType},
	{"increase(up[5m])", temporal.IncreaseType},
	{"resets(up[5m])", temporal.ResetsType},
	{"changes(up[5m])", temporal.ChangesType},
	{"holt_winters(up[5m], 0.2, 0.3)", temporal.HoltWintersType},
	{"predict_linear(up[5m], 100)", temporal.PredictLinearType},
	{"deriv(up[5m])", temporal.DerivType},
}

func TestTemporalParses(t *testing.T) {
	for _, tt := range temporalParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, models.NewTagOptions())
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

var tagParseTests = []struct {
	q            string
	expectedType string
}{
	{`label_join(up, "foo", ",", "s1","s2","s4")`, tag.TagJoinType},
	{`label_replace(up, "foo", "$1", "tagname","(.*):.*")`, tag.TagReplaceType},
}

func TestTagParses(t *testing.T) {
	for _, tt := range tagParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, models.NewTagOptions())
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

func TestFailedTemporalParse(t *testing.T) {
	q := "unknown_over_time(http_requests_total[5m])"
	_, err := Parse(q, models.NewTagOptions())
	require.Error(t, err)
}
