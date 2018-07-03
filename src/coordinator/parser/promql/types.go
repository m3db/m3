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

	"github.com/m3db/m3db/src/coordinator/functions"
	"github.com/m3db/m3db/src/coordinator/functions/logical"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/parser/common"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

// NewSelectorFromVector creates a new fetchop
func NewSelectorFromVector(n *promql.VectorSelector) (parser.Params, error) {
	matchers, err := labelMatchersToModelMatcher(n.LabelMatchers)
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
func NewSelectorFromMatrix(n *promql.MatrixSelector) (parser.Params, error) {
	matchers, err := labelMatchersToModelMatcher(n.LabelMatchers)
	if err != nil {
		return nil, err
	}

	return functions.FetchOp{Name: n.Name, Offset: n.Offset, Matchers: matchers, Range: n.Range}, nil
}

// NewOperator creates a new operator based on the type
func NewOperator(opType promql.ItemType) (parser.Params, error) {
	switch getOpType(opType) {
	case functions.CountType:
		return functions.CountOp{}, nil
	default:
		// TODO: handle other types
		return nil, fmt.Errorf("operator not supported: %s", opType)
	}
}

// NewBinaryOperator creates a new binary operator based on the type
func NewBinaryOperator(expr *promql.BinaryExpr, lhs parser.NodeID, rhs parser.NodeID) (parser.Params, error) {
	switch getOpType(expr.Op) {
	case logical.AndType:
		return logical.NewAndOp(lhs, rhs, promMatchingToM3(expr.VectorMatching)), nil
	default:
		// TODO: handle other types
		return nil, fmt.Errorf("operator not supported: %s", expr.Op)
	}
}

// NewFunctionExpr creates a new function expr based on the type
func NewFunctionExpr(name string) (parser.Params, error) {
	switch name {
	case functions.AbsType:
		return functions.AbsOp{}, nil
	default:
		// TODO: handle other types
		return nil, fmt.Errorf("function not supported: %s", name)
	}
}

func getOpType(opType promql.ItemType) string {
	switch opType {
	case promql.ItemType(itemCount):
		return functions.CountType
	case promql.ItemType(itemLAND):
		return logical.AndType
	default:
		return common.UnknownOpType
	}
}

func labelMatchersToModelMatcher(lMatchers []*labels.Matcher) (models.Matchers, error) {
	matchers := make(models.Matchers, len(lMatchers))
	for i, m := range lMatchers {
		modelType, err := promTypeToM3(m.Type)
		if err != nil {
			return nil, err
		}

		match, err := models.NewMatcher(modelType, m.Name, m.Value)
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

func promVectorCardinalityToM3(card promql.VectorMatchCardinality) logical.VectorMatchCardinality {
	switch card {
	case promql.CardOneToOne:
		return logical.CardOneToOne
	case promql.CardManyToMany:
		return logical.CardManyToMany
	case promql.CardManyToOne:
		return logical.CardManyToOne
	case promql.CardOneToMany:
		return logical.CardOneToMany
	}

	panic(fmt.Sprintf("unknown prom cardinality %d", card))
}

func promMatchingToM3(vectorMatching *promql.VectorMatching) *logical.VectorMatching {
	return &logical.VectorMatching{
		Card:           promVectorCardinalityToM3(vectorMatching.Card),
		MatchingLabels: vectorMatching.MatchingLabels,
		On:             vectorMatching.On,
		Include:        vectorMatching.Include,
	}
}
