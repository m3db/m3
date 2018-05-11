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
	"github.com/m3db/m3db/src/coordinator/functions"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/parser/common"

	"github.com/prometheus/prometheus/promql"
)

// NewSelectorFromVector creates a new fetchop
func NewSelectorFromVector(n *promql.VectorSelector) parser.Params {
	// TODO: convert n.LabelMatchers to Matchers
	return functions.FetchOp{Name: n.Name, Offset: n.Offset, Matchers: nil}
}

// NewSelectorFromMatrix creates a new fetchop
func NewSelectorFromMatrix(n *promql.MatrixSelector) parser.Params {
	// TODO: convert n.LabelMatchers to Matchers
	return functions.FetchOp{Name: n.Name, Offset: n.Offset, Matchers: nil, Range: n.Range}
}

// NewOperator creates a new operator based on the type
func NewOperator(opType promql.ItemType) parser.Params {
	switch getOpType(opType) {
	case functions.CountType:
		return functions.CountOp{}
	}
	// TODO: handle other types
	return nil
}

func getOpType(opType promql.ItemType) string {
	switch opType {
	case promql.ItemType(itemCount):
		return functions.CountType
	default:
		return common.UnknownOpType
	}
}
