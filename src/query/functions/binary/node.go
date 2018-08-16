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

package binary

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/logical"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// *******************************************
	// * Arithmetic functions

	// PlusType adds datapoints in both series
	PlusType = "+"

	// MinusType subtracts rhs from lhs
	MinusType = "-"

	// MultiplyType multiplies datapoints by series
	MultiplyType = "*"

	// DivType divides datapoints by series
	// Special cases are:
	// 	 X / 0 = +Inf
	// 	-X / 0 = -Inf
	// 	 0 / 0 =  NaN
	DivType = "/"

	// ExpType raises lhs to the power of rhs
	// NB: to keep consistency with prometheus (and go)
	//  0 ^ 0 = 1
	//  NaN ^ 0 = 1
	ExpType = "^"

	// ModType takes the modulo of lhs by rhs
	// Special cases are:
	// 	 X % 0 = NaN
	// 	 NaN % X = NaN
	// 	 X % NaN = NaN
	ModType = "%"

	// *******************************************
	// * Comparison functions

	// EqType raises lhs to the power of rhs
	EqType = "=="

	// NotEqType raises lhs to the power of rhs
	NotEqType = "!="

	// GreaterType raises lhs to the power of rhs
	GreaterType = ">"

	// LesserType raises lhs to the power of rhs
	LesserType = "<"

	// GreaterEqType raises lhs to the power of rhs
	GreaterEqType = ">="

	// LesserEqType raises lhs to the power of rhs
	LesserEqType = "<="
)

type mathFunc func(x, y float64) float64

type singleScalarFunc func(x float64) float64

func toFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

var (
	mathFuncs = map[string]mathFunc{
		PlusType:     func(x, y float64) float64 { return x + y },
		MinusType:    func(x, y float64) float64 { return x - y },
		MultiplyType: func(x, y float64) float64 { return x * y },
		DivType:      func(x, y float64) float64 { return x / y },
		ModType:      math.Mod,
		ExpType:      math.Pow,

		EqType:        func(x, y float64) float64 { return toFloat(x == y) },
		NotEqType:     func(x, y float64) float64 { return toFloat(x != y) },
		GreaterType:   func(x, y float64) float64 { return toFloat(x > y) },
		LesserType:    func(x, y float64) float64 { return toFloat(x < y) },
		GreaterEqType: func(x, y float64) float64 { return toFloat(x >= y) },
		LesserEqType:  func(x, y float64) float64 { return toFloat(x <= y) },
	}

	errLeftScalar              = errors.New("expected left scalar but node type incorrect")
	errRightScalar             = errors.New("expected right scalar but node type incorrect")
	errNoModifierForComparison = errors.New("comparisons between scalars must use BOOL modifier")
	errNoMatching              = errors.New("vector matching parameters must be provided for binary operations between series")
)

type binaryOp struct {
	OperatorType string
	fn           mathFunc
	params       NodeParams
}

// OpType for the operator
func (o binaryOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o binaryOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.params.LNode, o.params.RNode)
}

// Node creates an execution node
func (o binaryOp) Node(controller *transform.Controller) transform.OpNode {
	return &binaryNode{
		controller: controller,
		cache:      transform.NewBlockCache(),
		op:         o,
	}
}

type binaryNode struct {
	op         binaryOp
	controller *transform.Controller
	cache      *transform.BlockCache
	mu         sync.Mutex
}

// NodeParams describes the types of nodes used
// for binary operations
type NodeParams struct {
	LNode, RNode         parser.NodeID
	LIsScalar, RIsScalar bool
	ReturnBool           bool
	VectorMatching       *logical.VectorMatching
}

// NewBinaryOp creates a new binary operation
func NewBinaryOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	fn, ok := mathFuncs[opType]
	if !ok {
		return binaryOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return binaryOp{
		OperatorType: opType,
		fn:           fn,
		params:       params,
	}, nil
}

// Process processes a block
func (n *binaryNode) Process(ID parser.NodeID, b block.Block) error {
	lhs, rhs, err := n.computeOrCache(ID, b)
	if err != nil {
		// Clean up any blocks from cache
		n.cleanup()
		return err
	}

	// Both blocks are not ready
	if lhs == nil || rhs == nil {
		return nil
	}

	n.cleanup()

	nextBlock, err := n.process(lhs, rhs)
	if err != nil {
		return err
	}

	defer nextBlock.Close()
	return n.controller.Process(nextBlock)
}

// processes two logical blocks, performing a logical operation on them
func (n *binaryNode) process(lhs, rhs block.Block) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	fn := n.op.fn
	params := n.op.params
	if params.LIsScalar {
		scalarL, ok := lhs.(*block.Scalar)
		if !ok {
			return nil, errLeftScalar
		}

		lVal := scalarL.Value()

		// rhs is a series; use rhs metadata and series meta
		if !params.RIsScalar {
			return n.processSingleBlock(
				rhs,
				func(x float64) float64 {
					return n.op.fn(lVal, x)
				},
			)
		}

		// if both lhs and rhs are scalars, can create a new block
		// by extracting values from lhs and rhs instead of doing
		// by-value comparisons
		scalarR, ok := rhs.(*block.Scalar)
		if !ok {
			return nil, errRightScalar
		}

		// NB(arnikola): this is a sanity check, as scalar comparisons
		// should have previously errored out during the parsing step
		if !params.ReturnBool {
			return nil, errNoModifierForComparison
		}

		return block.NewScalar(
			fn(lVal, scalarR.Value()),
			lIter.Meta().Bounds,
		), nil
	}

	if params.RIsScalar {
		scalarR, ok := rhs.(*block.Scalar)
		if !ok {
			return nil, errRightScalar
		}

		rVal := scalarR.Value()
		// lhs is a series; use lhs metadata and series meta
		return n.processSingleBlock(
			lhs,
			func(x float64) float64 {
				return n.op.fn(x, rVal)
			},
		)
	}

	// both lhs and rhs are series
	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	// NB(arnikola): this is a sanity check, as functions between
	// two series missing vector matching should have previously
	// errored out during the parsing step
	if params.VectorMatching == nil {
		return nil, errNoMatching
	}

	return n.processBothSeries(lIter, rIter)
}

func isComparison(op string) bool {
	return op == EqType || op == NotEqType ||
		op == GreaterType || op == LesserType ||
		op == GreaterEqType || op == LesserEqType
}

// If returnBool is false and op is a comparison function,
// the function should return the scalar value rather than 1 or 0
func scalarFuncReturnBool(fn singleScalarFunc, op string, returnBool bool) singleScalarFunc {
	if returnBool || !isComparison(op) {
		return fn
	}
	return func(x float64) float64 {
		take := fn(x) == 1
		if take {
			return x
		}
		// NB(arnikola): whereas prometheus does not include not matching
		// points in the output, we here replace them with NaNs instead
		return math.NaN()
	}
}

func (n *binaryNode) processSingleBlock(
	block block.Block,
	fn singleScalarFunc,
) (block.Block, error) {
	returnBoolFn := scalarFuncReturnBool(fn, n.op.OperatorType, n.op.params.ReturnBool)

	it, err := block.StepIter()
	if err != nil {
		return nil, err
	}

	builder, err := n.controller.BlockBuilder(it.Meta(), it.SeriesMeta())
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(it.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; it.Next(); index++ {
		step, err := it.Current()
		if err != nil {
			return nil, err
		}

		values := step.Values()
		for _, value := range values {
			builder.AppendValue(index, returnBoolFn(value))
		}
	}

	return builder.Build(), nil
}

// If returnBool is false and op is a comparison function,
// the function should return the scalar value of lhs if the comparison
// is true; otherwise, it should return 1 if true, and 0 if false
func funcReturnBool(fn mathFunc, op string, returnBool bool) mathFunc {
	if returnBool || !isComparison(op) {
		return fn
	}
	return func(left, right float64) float64 {
		// NB (arnikola): safe to check that this equals 1 since this is
		// only performed on comparison functions where viable values
		// are only 1 or 0
		take := fn(left, right) == 1
		if take {
			return left
		}
		// NB(arnikola): whereas prometheus does not include not matching
		// points in the output, we here replace them with NaNs instead
		return math.NaN()
	}
}

func (n *binaryNode) processBothSeries(
	lIter, rIter block.StepIter,
) (block.Block, error) {
	lMeta, rMeta := lIter.Meta(), rIter.Meta()

	lSeriesMeta := logical.FlattenMetadata(lMeta, lIter.SeriesMeta())
	rSeriesMeta := logical.FlattenMetadata(rMeta, rIter.SeriesMeta())

	params := n.op.params
	takeLeft, correspondingRight, lSeriesMeta := intersect(params.VectorMatching, lSeriesMeta, rSeriesMeta)

	lMeta.Tags, lSeriesMeta = logical.DedupeMetadata(lSeriesMeta)

	// Use metas from only taken left series
	builder, err := n.controller.BlockBuilder(lMeta, lSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	returnBoolFn := funcReturnBool(n.op.fn, n.op.OperatorType, params.ReturnBool)

	for index := 0; lIter.Next() && rIter.Next(); index++ {
		lStep, err := lIter.Current()
		if err != nil {
			return nil, err
		}

		lValues := lStep.Values()
		rStep, err := rIter.Current()
		if err != nil {
			return nil, err
		}

		rValues := rStep.Values()

		for seriesIdx, lIdx := range takeLeft {
			rIdx := correspondingRight[seriesIdx]
			lVal := lValues[lIdx]
			rVal := rValues[rIdx]

			builder.AppendValue(index, returnBoolFn(lVal, rVal))
		}
	}

	return builder.Build(), nil
}

// computeOrCache figures out if both lhs and rhs are available, if not then it caches the incoming block
func (n *binaryNode) computeOrCache(ID parser.NodeID, b block.Block) (block.Block, block.Block, error) {
	var lhs, rhs block.Block
	n.mu.Lock()
	defer n.mu.Unlock()
	op := n.op
	params := op.params
	if params.LNode == ID {
		rBlock, ok := n.cache.Get(params.RNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		rhs = rBlock
		lhs = b
	} else if params.RNode == ID {
		lBlock, ok := n.cache.Get(params.LNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		lhs = lBlock
		rhs = b
	}

	return lhs, rhs, nil
}

func (n *binaryNode) cleanup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cache.Remove(n.op.params.LNode)
	n.cache.Remove(n.op.params.RNode)
}

const initIndexSliceLength = 10

// intersect returns the slice of lhs indices that are shared with rhs,
// the indices of the corresponding rhs values, and the metas for taken indices
func intersect(
	matching *logical.VectorMatching,
	lhs, rhs []block.SeriesMeta,
) ([]int, []int, []block.SeriesMeta) {
	idFunction := logical.HashFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[uint64]int, len(rhs))
	for idx, meta := range rhs {
		rightSigs[idFunction(meta.Tags)] = idx
	}

	takeLeft := make([]int, 0, initIndexSliceLength)
	correspondingRight := make([]int, 0, initIndexSliceLength)
	leftMetas := make([]block.SeriesMeta, 0, initIndexSliceLength)

	for lIdx, ls := range lhs {
		// If there's a matching entry in the left-hand side Vector, add the sample.
		id := idFunction(ls.Tags)
		if rIdx, ok := rightSigs[id]; ok {
			takeLeft = append(takeLeft, lIdx)
			correspondingRight = append(correspondingRight, rIdx)
			leftMetas = append(leftMetas, ls)
		}
	}

	return takeLeft, correspondingRight, leftMetas
}
