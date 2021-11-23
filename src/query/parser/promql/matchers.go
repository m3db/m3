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

	"github.com/m3db/m3/src/query/block"
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

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	promql "github.com/prometheus/prometheus/promql/parser"
)

// NewSelectorFromVector creates a new fetchop.
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

// NewSelectorFromMatrix creates a new fetchop.
func NewSelectorFromMatrix(
	n *promql.MatrixSelector,
	tagOpts models.TagOptions,
) (parser.Params, error) {
	vectorSelector := n.VectorSelector.(*promql.VectorSelector)
	matchers, err := LabelMatchersToModelMatcher(vectorSelector.LabelMatchers, tagOpts)
	if err != nil {
		return nil, err
	}

	return functions.FetchOp{
		Name:     vectorSelector.Name,
		Offset:   vectorSelector.Offset,
		Matchers: matchers,
		Range:    n.Range,
	}, nil
}

// NewAggregationOperator creates a new aggregation operator based on the type.
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
	switch op {
	case common.UnknownOpType:
		return nil, fmt.Errorf("operator not supported: %s", opType)

	case aggregation.BottomKType, aggregation.TopKType:
		val, err := resolveScalarArgument(expr.Param)
		if err != nil {
			return nil, err
		}

		nodeInformation.Parameter = val
		return aggregation.NewTakeOp(op, nodeInformation)

	case aggregation.CountValuesType:
		paren := unwrapParenExpr(expr.Param)
		val, err := resolveStringArgument(paren)
		if err != nil {
			return nil, err
		}
		nodeInformation.StringParameter = val
		return aggregation.NewCountValuesOp(op, nodeInformation)

	case aggregation.QuantileType:
		val, err := resolveScalarArgument(expr.Param)
		if err != nil {
			return nil, err
		}

		nodeInformation.Parameter = val
	}
	return aggregation.NewAggregationOp(op, nodeInformation)
}

func unwrapParenExpr(expr promql.Expr) promql.Expr {
	for {
		if paren, ok := expr.(*promql.ParenExpr); ok {
			expr = paren.Expr
		} else {
			return expr
		}
	}
}

func resolveStringArgument(expr promql.Expr) (string, error) {
	if expr == nil {
		return "", fmt.Errorf("expression is nil")
	}

	if str, ok := expr.(*promql.StringLiteral); ok {
		return str.Val, nil
	}

	return expr.String(), nil
}

func getAggOpType(opType promql.ItemType) string {
	switch opType {
	case promql.SUM:
		return aggregation.SumType
	case promql.MIN:
		return aggregation.MinType
	case promql.MAX:
		return aggregation.MaxType
	case promql.AVG:
		return aggregation.AverageType
	case promql.STDDEV:
		return aggregation.StandardDeviationType
	case promql.STDVAR:
		return aggregation.StandardVarianceType
	case promql.COUNT:
		return aggregation.CountType

	case promql.TOPK:
		return aggregation.TopKType
	case promql.BOTTOMK:
		return aggregation.BottomKType
	case promql.QUANTILE:
		return aggregation.QuantileType
	case promql.COUNT_VALUES:
		return aggregation.CountValuesType

	default:
		return common.UnknownOpType
	}
}

func newScalarOperator(
	expr *promql.NumberLiteral,
	tagOpts models.TagOptions,
) (parser.Params, error) {
	return scalar.NewScalarOp(expr.Val, tagOpts)
}

// NewBinaryOperator creates a new binary operator based on the type.
func NewBinaryOperator(expr *promql.BinaryExpr,
	lhs, rhs parser.NodeID) (parser.Params, error) {
	matcherBuilder := promMatchingToM3(expr.VectorMatching)
	nodeParams := binary.NodeParams{
		LNode:                lhs,
		RNode:                rhs,
		ReturnBool:           expr.ReturnBool,
		VectorMatcherBuilder: matcherBuilder,
	}

	op := getBinaryOpType(expr.Op)
	return binary.NewOp(op, nodeParams)
}

var dateFuncs = []string{linear.DayOfMonthType, linear.DayOfWeekType,
	linear.DaysInMonthType, linear.HourType, linear.MinuteType,
	linear.MonthType, linear.YearType}

func isDateFunc(name string) bool {
	for _, n := range dateFuncs {
		if name == n {
			return true
		}
	}

	return false
}

// NewFunctionExpr creates a new function expr based on the type.
func NewFunctionExpr(
	name string,
	argValues []interface{},
	stringValues []string,
	hasArgValue bool,
	inner string,
	tagOptions models.TagOptions,
) (parser.Params, bool, error) {
	var (
		p   parser.Params
		err error
	)

	if isDateFunc(name) {
		p, err = linear.NewDateOp(name, hasArgValue)
		return p, true, err
	}

	switch name {
	case linear.AbsType, linear.CeilType, linear.ExpType,
		linear.FloorType, linear.LnType, linear.Log10Type,
		linear.Log2Type, linear.SqrtType:
		p, err = linear.NewMathOp(name)
		return p, true, err

	case aggregation.AbsentType:
		p = aggregation.NewAbsentOp()
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

	case tag.TagJoinType, tag.TagReplaceType:
		p, err = tag.NewTagOp(name, stringValues)
		return p, true, err

	case temporal.AvgType, temporal.CountType, temporal.MinType,
		temporal.MaxType, temporal.SumType, temporal.StdDevType,
		temporal.StdVarType, temporal.LastType:
		p, err = temporal.NewAggOp(argValues, name)
		return p, true, err

	case temporal.QuantileType:
		p, err = temporal.NewQuantileOp(argValues, name)
		return p, true, err

	case temporal.HoltWintersType:
		p, err = temporal.NewHoltWintersOp(argValues)
		return p, true, err

	case temporal.IRateType, temporal.IDeltaType,
		temporal.RateType, temporal.IncreaseType,
		temporal.DeltaType:
		p, err = temporal.NewRateOp(argValues, name)
		return p, true, err

	case temporal.PredictLinearType, temporal.DerivType:
		p, err = temporal.NewLinearRegressionOp(argValues, name)
		return p, true, err

	case temporal.ResetsType, temporal.ChangesType:
		p, err = temporal.NewFunctionOp(argValues, name)
		return p, true, err

	case unconsolidated.TimestampType:
		p, err = unconsolidated.NewTimestampOp(name)
		return p, true, err

	case scalar.TimeType:
		p, err = scalar.NewTimeOp(tagOptions)
		return p, true, err

	case linear.SortType, linear.SortDescType:
		p, err = linear.NewSortOp(name)
		return p, true, err

	// NB: no-ops.
	case scalar.ScalarType:
		return nil, false, err

	default:
		return nil, false, fmt.Errorf("function not supported: %s", name)
	}
}

func getBinaryOpType(opType promql.ItemType) string {
	switch opType {
	case promql.LAND:
		return binary.AndType
	case promql.LOR:
		return binary.OrType
	case promql.LUNLESS:
		return binary.UnlessType

	case promql.ADD:
		return binary.PlusType
	case promql.SUB:
		return binary.MinusType
	case promql.MUL:
		return binary.MultiplyType
	case promql.DIV:
		return binary.DivType
	case promql.POW:
		return binary.ExpType
	case promql.MOD:
		return binary.ModType

	case promql.EQL, promql.EQLC:
		return binary.EqType
	case promql.NEQ:
		return binary.NotEqType
	case promql.GTR:
		return binary.GreaterType
	case promql.LSS:
		return binary.LesserType
	case promql.GTE:
		return binary.GreaterEqType
	case promql.LTE:
		return binary.LesserEqType

	default:
		return common.UnknownOpType
	}
}

// getUnaryOpType returns the M3 unary op type based on the Prom op type.
func getUnaryOpType(opType promql.ItemType) (string, error) {
	switch opType {
	case promql.ADD:
		return binary.PlusType, nil
	case promql.SUB:
		return binary.MinusType, nil
	default:
		return "", fmt.Errorf(
			"only + and - operators allowed for unary expressions, received: %s",
			opType.String(),
		)
	}
}

const (
	anchorStart = byte('^')
	anchorEnd   = byte('$')
	escapeChar  = byte('\\')
	startGroup  = byte('[')
	endGroup    = byte(']')
)

func sanitizeRegex(value []byte) []byte {
	lIndex := 0
	rIndex := len(value)
	escape := false
	inGroup := false
	for i, b := range value {
		if escape {
			escape = false
			continue
		}

		if inGroup {
			switch b {
			case escapeChar:
				escape = true
			case endGroup:
				inGroup = false
			}

			continue
		}

		switch b {
		case anchorStart:
			lIndex = i + 1
		case anchorEnd:
			rIndex = i
		case escapeChar:
			escape = true
		case startGroup:
			inGroup = true
		}
	}

	if lIndex > rIndex {
		return []byte{}
	}

	return value[lIndex:rIndex]
}

// LabelMatchersToModelMatcher parses promql matchers to model matchers.
func LabelMatchersToModelMatcher(
	lMatchers []*labels.Matcher,
	tagOpts models.TagOptions,
) (models.Matchers, error) {
	matchers := make(models.Matchers, 0, len(lMatchers))
	for _, m := range lMatchers {
		matchType, err := promTypeToM3(m.Type)
		if err != nil {
			return nil, err
		}

		var name []byte
		if m.Name == model.MetricNameLabel {
			name = tagOpts.MetricName()
		} else {
			name = []byte(m.Name)
		}

		value := []byte(m.Value)
		// NB: special case here since by Prometheus convention, a NEQ tag with no
		// provided value is interpreted as verifying that the tag exists.
		// Similarily, EQ tag with no provided value is interpreted as ensuring that
		// the tag does not exist.
		if len(value) == 0 {
			if matchType == models.MatchNotEqual {
				matchType = models.MatchField
			} else if matchType == models.MatchEqual {
				matchType = models.MatchNotField
			}
		}

		if matchType == models.MatchRegexp || matchType == models.MatchNotRegexp {
			// NB: special case here since tags such as `{foo=~"$bar"}` are valid in
			// prometheus regex patterns, but invalid with m3 index queries. Simplify
			// these matchers here.
			value = sanitizeRegex(value)
		}

		match, err := models.NewMatcher(matchType, name, value)
		if err != nil {
			return nil, err
		}

		matchers = append(matchers, match)
	}

	return matchers, nil
}

// promTypeToM3 converts a prometheus label type to m3 matcher type.
// TODO(nikunj): Consider merging with prompb code.
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

func promVectorCardinalityToM3(
	card promql.VectorMatchCardinality,
) binary.VectorMatchCardinality {
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

func promMatchingToM3(
	vectorMatching *promql.VectorMatching,
) binary.VectorMatcherBuilder {
	// vectorMatching can be nil iff at least one of the sides is a scalar.
	if vectorMatching == nil {
		return nil
	}

	byteMatchers := make([][]byte, len(vectorMatching.MatchingLabels))
	for i, label := range vectorMatching.MatchingLabels {
		byteMatchers[i] = []byte(label)
	}

	return func(_, _ block.Block) binary.VectorMatching {
		return binary.VectorMatching{
			Set:            true,
			Card:           promVectorCardinalityToM3(vectorMatching.Card),
			MatchingLabels: byteMatchers,
			On:             vectorMatching.On,
			Include:        vectorMatching.Include,
		}
	}
}
