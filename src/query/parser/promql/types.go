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
	"time"

	"github.com/m3db/m3/src/query/functions"
	"github.com/m3db/m3/src/query/functions/aggregation"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/functions/linear"
	"github.com/m3db/m3/src/query/functions/scalar"
	"github.com/m3db/m3/src/query/functions/tag"
	"github.com/m3db/m3/src/query/functions/temporal"
	"github.com/m3db/m3/src/query/functions/unconsolidated"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/parser/common"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

// NewSelectorFromVector creates a new fetchop
func NewSelectorFromVector(
	n *promql.VectorSelector,
	tagOpts models.TagOptions,
) (parser.Params, error) {
	matchers, err := LabelMatchersToModelMatcher(n.LabelMatchers, tagOpts)
	if err != nil {
		return nil, err
	}

	return functions.FetchOp{
		Name:     n.Name,
		Offset:   n.Offset,
		Matchers: matchers,
	}, nil
}

// NewSelectorFromMatrix creates a new fetchop
func NewSelectorFromMatrix(
	n *promql.MatrixSelector,
	tagOpts models.TagOptions,
) (parser.Params, error) {
	matchers, err := LabelMatchersToModelMatcher(n.LabelMatchers, tagOpts)
	if err != nil {
		return nil, err
	}

	return functions.FetchOp{
		Name:     n.Name,
		Offset:   n.Offset,
		Matchers: matchers,
		Range:    n.Range,
	}, nil
}

// NewAggregationOperator creates a new aggregation operator based on the type
func NewAggregationOperator(expr *promql.AggregateExpr) (parser.Params, error) {
	opType := expr.Op
	byteMatchers := make([][]byte, len(expr.Grouping))
	for i, grouping := range expr.Grouping {
		byteMatchers[i] = []byte(grouping)
	}

	nodeInformation := aggregation.NodeParams{
		MatchingTags: byteMatchers,
		Without:      expr.Without,
	}

	op := getAggOpType(opType)
	if op == common.UnknownOpType {
		return nil, fmt.Errorf("operator not supported: %s", opType)
	}

	if op == aggregation.BottomKType || op == aggregation.TopKType {
		val, err := resolveScalarArgument(expr.Param)
		if err != nil {
			return nil, err
		}

		nodeInformation.Parameter = val
		return aggregation.NewTakeOp(op, nodeInformation)
	}

	if op == aggregation.CountValuesType {
		nodeInformation.StringParameter = expr.Param.String()
		return aggregation.NewCountValuesOp(op, nodeInformation)
	}

	return aggregation.NewAggregationOp(op, nodeInformation)
}

func getAggOpType(opType promql.ItemType) string {
	switch opType {
	case promql.ItemType(itemSum):
		return aggregation.SumType
	case promql.ItemType(itemMin):
		return aggregation.MinType
	case promql.ItemType(itemMax):
		return aggregation.MaxType
	case promql.ItemType(itemAvg):
		return aggregation.AverageType
	case promql.ItemType(itemStddev):
		return aggregation.StandardDeviationType
	case promql.ItemType(itemStdvar):
		return aggregation.StandardVarianceType
	case promql.ItemType(itemCount):
		return aggregation.CountType

	case promql.ItemType(itemTopK):
		return aggregation.TopKType
	case promql.ItemType(itemBottomK):
		return aggregation.BottomKType
	case promql.ItemType(itemQuantile):
		return aggregation.QuantileType
	case promql.ItemType(itemCountValues):
		return aggregation.CountValuesType
	default:
		return common.UnknownOpType
	}
}

// NewScalarOperator creates a new scalar operator
func NewScalarOperator(expr *promql.NumberLiteral) (parser.Params, error) {
	return scalar.NewScalarOp(
		func(_ time.Time) float64 { return expr.Val },
		scalar.ScalarType,
	)
}

// NewBinaryOperator creates a new binary operator based on the type
func NewBinaryOperator(expr *promql.BinaryExpr, lhs, rhs parser.NodeID) (parser.Params, error) {
	matching := promMatchingToM3(expr.VectorMatching)

	nodeParams := binary.NodeParams{
		LNode:          lhs,
		RNode:          rhs,
		LIsScalar:      expr.LHS.Type() == promql.ValueTypeScalar,
		RIsScalar:      expr.RHS.Type() == promql.ValueTypeScalar,
		ReturnBool:     expr.ReturnBool,
		VectorMatching: matching,
	}

	op := getBinaryOpType(expr.Op)
	return binary.NewOp(op, nodeParams)
}

// NewFunctionExpr creates a new function expr based on the type
func NewFunctionExpr(
	name string,
	argValues []interface{},
	stringValues []string,
) (parser.Params, bool, error) {
	var p parser.Params
	var err error

	switch name {
	case linear.AbsType, linear.CeilType, linear.ExpType, linear.FloorType, linear.LnType,
		linear.Log10Type, linear.Log2Type, linear.SqrtType:
		p, err = linear.NewMathOp(name)
		return p, true, err

	case linear.AbsentType:
		p = linear.NewAbsentOp()
		return p, true, err

	case linear.ClampMinType, linear.ClampMaxType:
		p, err = linear.NewClampOp(argValues, name)
		return p, true, err

	case linear.HistogramQuantileType:
		p, err = linear.NewHistogramQuantileOp(argValues, name)
		return p, true, err

	case linear.RoundType:
		p, err = linear.NewRoundOp(argValues)
		return p, true, err

	case linear.DayOfMonthType, linear.DayOfWeekType, linear.DaysInMonthType, linear.HourType,
		linear.MinuteType, linear.MonthType, linear.YearType:
		p, err = linear.NewDateOp(name)
		return p, true, err

	case tag.TagJoinType, tag.TagReplaceType:
		p, err = tag.NewTagOp(name, stringValues)
		return p, true, err

	case temporal.AvgType, temporal.CountType, temporal.MinType,
		temporal.MaxType, temporal.SumType, temporal.StdDevType,
		temporal.StdVarType:
		p, err = temporal.NewAggOp(argValues, name)
		return p, true, err

	case temporal.QuantileType:
		p, err = temporal.NewQuantileOp(argValues, name)
		return p, true, err

	case temporal.HoltWintersType:
		p, err = temporal.NewHoltWintersOp(argValues)
		return p, true, err

	case temporal.IRateType, temporal.IDeltaType, temporal.RateType, temporal.IncreaseType,
		temporal.DeltaType:
		p, err = temporal.NewRateOp(argValues, name)
		return p, true, err

	case temporal.PredictLinearType, temporal.DerivType:
		p, err = temporal.NewLinearRegressionOp(argValues, name)
		return p, true, err

	case temporal.ResetsType, temporal.ChangesType:
		p, err = temporal.NewFunctionOp(argValues, name)
		return p, true, err

	case linear.SortType, linear.SortDescType:
		return nil, false, err

	case scalar.ScalarType:
		return nil, false, err

	case unconsolidated.TimestampType:
		p, err = unconsolidated.NewTimestampOp(name)
		return p, true, err

	case scalar.TimeType:
		p, err = scalar.NewScalarOp(func(t time.Time) float64 { return float64(t.Unix()) }, scalar.TimeType)
		return p, true, err

	default:
		// TODO: handle other types
		return nil, false, fmt.Errorf("function not supported: %s", name)
	}
}

func getBinaryOpType(opType promql.ItemType) string {
	switch opType {
	case promql.ItemType(itemLAND):
		return binary.AndType
	case promql.ItemType(itemLOR):
		return binary.OrType
	case promql.ItemType(itemLUnless):
		return binary.UnlessType

	case promql.ItemType(itemADD):
		return binary.PlusType
	case promql.ItemType(itemSUB):
		return binary.MinusType
	case promql.ItemType(itemMUL):
		return binary.MultiplyType
	case promql.ItemType(itemDIV):
		return binary.DivType
	case promql.ItemType(itemPOW):
		return binary.ExpType
	case promql.ItemType(itemMOD):
		return binary.ModType

	case promql.ItemType(itemEQL):
		return binary.EqType
	case promql.ItemType(itemNEQ):
		return binary.NotEqType
	case promql.ItemType(itemGTR):
		return binary.GreaterType
	case promql.ItemType(itemLSS):
		return binary.LesserType
	case promql.ItemType(itemGTE):
		return binary.GreaterEqType
	case promql.ItemType(itemLTE):
		return binary.LesserEqType

	default:
		return common.UnknownOpType
	}
}

// GetUnaryOpType returns the M3 unary op type based on the Prom op type
func GetUnaryOpType(opType promql.ItemType) (string, bool) {
	switch opType {
	case promql.ItemType(itemADD):
		return binary.PlusType, true
	case promql.ItemType(itemSUB):
		return binary.MinusType, true
	default:
		return common.UnknownOpType, false
	}
}

const promDefaultName = "__name__"

// LabelMatchersToModelMatcher parses promql matchers to model matchers
func LabelMatchersToModelMatcher(
	lMatchers []*labels.Matcher,
	tagOpts models.TagOptions,
) (models.Matchers, error) {
	matchers := make(models.Matchers, len(lMatchers))
	for i, m := range lMatchers {
		modelType, err := promTypeToM3(m.Type)
		if err != nil {
			return nil, err
		}

		var name []byte
		if m.Name == promDefaultName {
			name = tagOpts.MetricName()
		} else {
			name = []byte(m.Name)
		}

		match, err := models.NewMatcher(modelType, name, []byte(m.Value))
		if err != nil {
			return nil, err
		}

		matchers[i] = match
	}

	return matchers, nil
}

// promTypeToM3 converts a prometheus label type to m3 matcher type
//TODO(nikunj): Consider merging with prompb code
func promTypeToM3(labelType labels.MatchType) (models.MatchType, error) {
	switch labelType {
	case labels.MatchEqual:
		return models.MatchEqual, nil
	case labels.MatchNotEqual:
		return models.MatchNotEqual, nil
	case labels.MatchRegexp:
		return models.MatchRegexp, nil
	case labels.MatchNotRegexp:
		return models.MatchNotRegexp, nil

	default:
		return 0, fmt.Errorf("unknown match type %v", labelType)
	}
}

func promVectorCardinalityToM3(card promql.VectorMatchCardinality) binary.VectorMatchCardinality {
	switch card {
	case promql.CardOneToOne:
		return binary.CardOneToOne
	case promql.CardManyToMany:
		return binary.CardManyToMany
	case promql.CardManyToOne:
		return binary.CardManyToOne
	case promql.CardOneToMany:
		return binary.CardOneToMany
	}

	panic(fmt.Sprintf("unknown prom cardinality %d", card))
}

func promMatchingToM3(vectorMatching *promql.VectorMatching) *binary.VectorMatching {
	// vectorMatching can be nil iff at least one of the sides is a scalar
	if vectorMatching == nil {
		return nil
	}

	byteMatchers := make([][]byte, len(vectorMatching.MatchingLabels))
	for i, label := range vectorMatching.MatchingLabels {
		byteMatchers[i] = []byte(label)
	}

	return &binary.VectorMatching{
		Card:           promVectorCardinalityToM3(vectorMatching.Card),
		MatchingLabels: byteMatchers,
		On:             vectorMatching.On,
		Include:        vectorMatching.Include,
	}
}
