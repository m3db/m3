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
	"fmt"
	"testing"
	"time"

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

	pql "github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAGWithCountOp(t *testing.T) {
	q := "count(http_requests_total{method=\"GET\"}) by (service)"
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[1].Op.OpType(), aggregation.CountType)
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"),
		"fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"),
		"aggregation should be the child")
}

func TestDAGWithOffset(t *testing.T) {
	q := "up offset 2m"
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 2)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
	assert.Equal(t, transforms[1].Op.OpType(), lazy.OffsetType)
	assert.Len(t, edges, 1)
	assert.Equal(t, edges[0].ParentID, parser.NodeID("0"),
		"fetch should be the parent")
	assert.Equal(t, edges[0].ChildID, parser.NodeID("1"),
		"offset should be the child")
}

func TestInvalidOffset(t *testing.T) {
	q := "up offset -2m"
	_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
}

func TestNegativeUnary(t *testing.T) {
	q := "-up"
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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

func TestPositiveUnary(t *testing.T) {
	q := "+up"
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 1) // "+" defaults to just a fetch operation
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Len(t, edges, 0)
}

func TestInvalidUnary(t *testing.T) {
	q := "*up"
	_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.Error(t, err)
}

func TestGetUnaryOpType(t *testing.T) {
	unaryOpType, err := getUnaryOpType(pql.ADD)
	require.NoError(t, err)
	assert.Equal(t, binary.PlusType, unaryOpType)

	_, err = getUnaryOpType(pql.EQL)
	require.Error(t, err)
}

func TestDAGWithEmptyExpression(t *testing.T) {
	q := ""
	_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.Error(t, err)
}

func TestDAGWithFakeOp(t *testing.T) {
	q := "fake(http_requests_total{method=\"GET\"})"
	_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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

	{"absent(up)", aggregation.AbsentType},
}

func TestAggregateParses(t *testing.T) {
	for _, tt := range aggregateParseTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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

var aggregationWithTagListTests = []string{
	// different number of tags
	"sum(up) by (t1,)",
	"sum(up) by (t1,t2)",
	"sum(up) without (t1)",
	"sum(up) without (t1, t2, t3)",

	// trailing comma in tag list
	"sum(up) by (t1,)",
	"sum(up) without (t1, t2,)",

	// alternative form
	"sum by (t) (up)",
	"sum by (t,) (up)",
	"sum without (t) (up)",
	"sum without (t,) (up)",
}

func TestAggregationWithTagListDoesNotError(t *testing.T) {
	for _, q := range aggregationWithTagListTests {
		t.Run(q, func(t *testing.T) {
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			_, _, err = p.DAG()
			require.NoError(t, err)
		})
	}
}

var linearParseTests = []struct {
	q            string
	expectedType string
}{
	{"abs(up)", linear.AbsType},
	{"ceil(up)", linear.CeilType},
	{"clamp_min(up, 1)", linear.ClampMinType},
	{"clamp_max(up, 1)", linear.ClampMaxType},
	{"exp(up)", linear.ExpType},
	{"floor(up)", linear.FloorType},
	{"ln(up)", linear.LnType},
	{"log2(up)", linear.Log2Type},
	{"log10(up)", linear.Log10Type},
	{"sqrt(up)", linear.SqrtType},
	{"round(up)", linear.RoundType},
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
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			require.Len(t, transforms, 2)
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

var variadicTests = []struct {
	q            string
	expectedType string
}{
	{"day_of_month()", linear.DayOfMonthType},
	{"day_of_week()", linear.DayOfWeekType},
	{"day_of_month()", linear.DayOfMonthType},
	{"days_in_month()", linear.DaysInMonthType},

	{"hour()", linear.HourType},
	{"minute()", linear.MinuteType},
	{"month()", linear.MonthType},
	{"year()", linear.YearType},
}

func TestVariadicParses(t *testing.T) {
	for _, tt := range variadicTests {
		t.Run(tt.q, func(t *testing.T) {
			q := tt.q
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			transforms, _, err := p.DAG()
			require.NoError(t, err)
			require.Len(t, transforms, 1)
			assert.Equal(t, transforms[0].Op.OpType(), tt.expectedType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
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
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			assert.Len(t, transforms, 2)
			assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
			assert.Equal(t, transforms[1].Op.OpType(), tt.expectedType)
			assert.Equal(t, transforms[1].ID, parser.NodeID("1"))
			assert.Len(t, edges, 1)
		})
	}
}

func TestScalar(t *testing.T) {
	p, err := Parse("scalar(up)", time.Second,
		models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
	transforms, edges, err := p.DAG()
	require.NoError(t, err)
	assert.Len(t, transforms, 1)
	assert.Equal(t, transforms[0].Op.OpType(), functions.FetchType)
	assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
	assert.Len(t, edges, 0)
}

func TestVector(t *testing.T) {
	vectorExprs := []string{
		"vector(12)",
		"vector(scalar(up))",
		"vector(12 - scalar(vector(100)-2))",
	}

	for _, expr := range vectorExprs {
		t.Run(expr, func(t *testing.T) {
			p, err := Parse(expr, time.Second,
				models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			transforms, edges, err := p.DAG()
			require.NoError(t, err)
			assert.Len(t, transforms, 1)
			assert.Equal(t, transforms[0].Op.OpType(), scalar.ScalarType)
			assert.Equal(t, transforms[0].ID, parser.NodeID("0"))
			assert.Len(t, edges, 0)
		})
	}
}

func TestTimeTypeParse(t *testing.T) {
	q := "time()"
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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

	// Various spacing
	{"up/ up", functions.FetchType, functions.FetchType, binary.DivType},
	{"up-up", functions.FetchType, functions.FetchType, binary.MinusType},
	{"10 -up", scalar.ScalarType, functions.FetchType, binary.MinusType},
	{"up*10", functions.FetchType, scalar.ScalarType, binary.MultiplyType},
	{"up!=10", functions.FetchType, scalar.ScalarType, binary.NotEqType},
	{"10   <up", scalar.ScalarType, functions.FetchType, binary.LesserType},
	{"up>=   10", functions.FetchType, scalar.ScalarType, binary.GreaterEqType},
}

func TestBinaryParses(t *testing.T) {
	for _, tt := range binaryParseTests {
		t.Run(tt.q, func(t *testing.T) {
			p, err := Parse(tt.q, time.Second,
				models.NewTagOptions(), NewParseOptions())

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
	p, err := Parse("(5^(up-6))", time.Second,
		models.NewTagOptions(), NewParseOptions())
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
	{"last_over_time(up[5m])", temporal.LastType},
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
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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
			p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
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
	_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.Error(t, err)
}

func TestMissingTagsDoNotPanic(t *testing.T) {
	q := `label_join(up, "foo", ",")`
	p, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
	require.NoError(t, err)
	assert.NotPanics(t, func() { _, _, _ = p.DAG() })
}

var functionArgumentExpressionTests = []struct {
	name string
	q    string
}{
	{
		"scalar argument",
		"vector(((1)))",
	},
	{
		"string argument",
		`label_join(up, ("foo"), ((",")), ((("bar"))))`,
	},
	{
		"vector argument",
		"abs(((foo)))",
	},
	{
		"matrix argument",
		"stddev_over_time(((metric[1m])))",
	},
}

func TestExpressionsInFunctionArgumentsDoNotError(t *testing.T) {
	for _, tt := range functionArgumentExpressionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Parse(tt.q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.NoError(t, err)
			_, _, err = p.DAG()
			require.NoError(t, err)
		})
	}
}

var invalidFunctionArgumentsTests = []string{
	"vector(())",
	"vector((1)",
	"vector(metric)",
	`label_join(up, "f" + "oo", ",", "ba" + "r")`,
	`label_join(up, 1, ",", 2)`,
	`label_join("up", "foo", ",", "bar")`,
	"abs(1)",
	"abs(())",
	"stddev_over_time(metric[1m]+1)",
	"stddev_over_time(metric)",
}

func TestParseInvalidFunctionArgumentsErrors(t *testing.T) {
	for _, q := range invalidFunctionArgumentsTests {
		t.Run(q, func(t *testing.T) {
			_, err := Parse(q, time.Second, models.NewTagOptions(), NewParseOptions())
			require.Error(t, err)
		})
	}
}

func TestCustomParseOptions(t *testing.T) {
	q := "query"
	v := "foo"
	called := 0
	fn := func(query string) (pql.Expr, error) {
		assert.Equal(t, q, query)
		called++
		return &pql.StringLiteral{Val: v}, nil
	}

	opts := NewParseOptions().SetParseFn(fn)
	ex, err := Parse(q, time.Second, models.NewTagOptions(), opts)
	require.NoError(t, err)
	assert.Equal(t, 1, called)
	parse, ok := ex.(*promParser)
	require.True(t, ok)
	assert.Equal(t, pql.ValueTypeString, parse.expr.Type())
	str, ok := parse.expr.(*pql.StringLiteral)
	require.True(t, ok)
	assert.Equal(t, v, str.Val)
}

type customParam struct {
	prefix string
}

func (c customParam) String() string {
	return fmt.Sprintf("%s_custom", c.prefix)
}

func (c customParam) OpType() string {
	return fmt.Sprintf("%s_customOpType", c.prefix)
}

func TestCustomSort(t *testing.T) {
	tests := []struct {
		q  string
		ex string
	}{
		{"sort(up)", "sort_customOpType"},
		{"clamp_max(up, 0.3)", "clamp_max_customOpType"},
	}

	fn := func(s string, _ []interface{}, _ []string,
		_ bool, _ string, _ models.TagOptions) (parser.Params, bool, error) {
		return customParam{s}, true, nil
	}

	opts := NewParseOptions().SetFunctionParseExpr(fn)
	for _, tt := range tests {
		p, err := Parse(tt.q, time.Second, models.NewTagOptions(), opts)
		require.NoError(t, err)
		transforms, edges, err := p.DAG()
		require.NoError(t, err)
		require.Len(t, transforms, 2)
		assert.Equal(t, functions.FetchType, transforms[0].Op.OpType())
		assert.Equal(t, parser.NodeID("0"), transforms[0].ID)
		assert.Len(t, edges, 1)
		assert.Equal(t, tt.ex, transforms[1].Op.OpType())
		assert.Equal(t, parser.NodeID("1"), transforms[1].ID)
	}
}
