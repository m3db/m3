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

package functions

import (
	"fmt"
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	// AndType uses values from lhs for which there is a value in rhs
	AndType = "and"
)

// VectorMatchCardinality describes the cardinality relationship
// of two Vectors in a binary operation.
type VectorMatchCardinality int

const (
	CardOneToOne   VectorMatchCardinality = iota
	CardManyToOne
	CardOneToMany
	CardManyToMany
)

// VectorMatching describes how elements from two Vectors in a binary
// operation are supposed to be matched.
type VectorMatching struct {
	// The cardinality of the two Vectors.
	Card VectorMatchCardinality
	// MatchingLabels contains the labels which define equality of a pair of
	// elements from the Vectors.
	MatchingLabels []string
	// On includes the given label names from matching,
	// rather than excluding them.
	On bool
	// Include contains additional labels that should be included in
	// the result from the side with the lower cardinality.
	Include []string
}

// AbsOp stores required properties for abs
type LogicalOp struct {
	OperatorType string
	LNode        parser.NodeID
	RNode        parser.NodeID
	Matching     *VectorMatching
	ReturnBool   bool
}

// OpType for the operator
func (o LogicalOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o LogicalOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.LNode, o.RNode)
}

// Node creates an execution node
func (o LogicalOp) Node(controller *transform.Controller) transform.OpNode {
	return &LogicalNode{op: o, controller: controller, cache: transform.NewBlockCache()}
}

// LogicalNode is an execution node
type LogicalNode struct {
	op         LogicalOp
	controller *transform.Controller
	cache      *transform.BlockCache
}

// Process the block
func (c *LogicalNode) Process(ID parser.NodeID, b block.Block) error {
	var lhs block.Block
	var rhs block.Block
	if c.op.LNode == ID {
		rBlock, ok := c.cache.Get(c.op.RNode)
		if !ok {
			c.cache.Add(ID, b)
			return nil
		}

		rhs = rBlock
		lhs = b
	} else if c.op.RNode == ID {
		lBlock, ok := c.cache.Get(c.op.LNode)
		if !ok {
			c.cache.Add(ID, b)
			return nil
		}

		lhs = lBlock
		rhs = b
	}

	intersection := c.intersect(lhs.SeriesMeta(), rhs.SeriesMeta(), c.op.Matching)
	builder, err := c.controller.BlockBuilder(b.Meta(), b.SeriesMeta())
	if err != nil {
		return err
	}

	lIter := lhs.StepIter()
	if err := builder.AddCols(lhs.StepCount()); err != nil {
		return err
	}

	for index := 0; lIter.Next(); index++ {
		lStep := lIter.Current()
		lValues := lStep.Values()

		for idx, value := range lValues {
			rIdx := intersection[idx]
			if rIdx < 0 {
				builder.AppendValue(index, math.NaN())
				continue
			}

			builder.AppendValue(index, value)
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}

func (c *LogicalNode) intersect(lhs []block.SeriesMeta, rhs []block.SeriesMeta, matching *VectorMatching) []int {
	sigf := signatureFunc(matching.On, matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[string]int, len(lhs))
	matches := make([]int, len(lhs))
	for idx, meta := range rhs {
		rightSigs[sigf(meta.Tags)] = idx
	}

	for i, ls := range lhs {
		// If there's a matching entry in the right-hand side Vector, add the sample.
		if idx, ok := rightSigs[sigf(ls.Tags)]; ok {
			matches[i] = idx
		}
	}

	return matches
}

// signatureFunc returns a function that calculates the signature for a metric
// ignoring the provided labels. If on, then the given labels are only used instead.
func signatureFunc(on bool, names ...string) func(models.Tags) string {
	// TODO(fabxc): ensure names are sorted and then use that and sortedness
	// of labels by names to speed up the operations below.
	// Alternatively, inline the hashing and don't build new label sets.
	if on {
		return func(lset models.Tags) string { return hashForLabels(lset, names...) }
	}
	return func(lset models.Tags) string { return hashWithoutLabels(lset, names...) }
}

func hashForLabels(lset models.Tags, names ...string) string {
	cm := make(models.Tags, len(names))
	for key, val := range lset {
		for _, n := range names {
			if key == n {
				cm[key] = val
				break
			}
		}
	}
	return cm.ID()
}

func hashWithoutLabels(lset models.Tags, names ...string) string {
	cm := make(models.Tags, len(lset))

	for key, val := range lset {
		if key == labels.MetricName {
			continue
		}

		found := false
		for _, n := range names {
			if n == key {
				found = true
				break
			}
		}

		if !found {
			cm[key] = val
		}
	}

	return cm.ID()
}
