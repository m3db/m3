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

package aggregation

import (
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/models"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
)

// CountType counts all non nan elements in a list of series
const CountType = "count"

// CountOp stores required properties for count
type CountOp struct {
}

// OpType for the operator
func (o CountOp) OpType() string {
	return CountType
}

// String representation
func (o CountOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o CountOp) Node(controller *transform.Controller) transform.OpNode {
	return &CountNode{op: o, controller: controller}
}

// CountNode is an execution node
type CountNode struct {
	op         CountOp
	controller *transform.Controller
	matching   []string
	without    bool
}

// Process the block
func (c *CountNode) Process(ID parser.NodeID, b block.Block) error {
	// TODO: Figure out a good name and tags after an aggregation operation
	meta := block.SeriesMeta{
		Name: CountType,
	}

	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	builder, err := c.controller.BlockBuilder(stepIter.Meta(), []block.SeriesMeta{meta})
	if err != nil {
		return err
	}

	if err := builder.AddCols(stepIter.StepCount()); err != nil {
		return err
	}

	for index := 0; stepIter.Next(); index++ {
		step, err := stepIter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		count := 0.0
		for _, value := range values {
			if !math.IsNaN(value) {
				count++
			}
		}

		builder.AppendValue(index, count)
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}

type withKeysID func(tags models.Tags, matching []string) uint64

func includeKeysID(tags models.Tags, matching []string) uint64 { return tags.IDWithKeys(matching...) }
func excludeKeysID(tags models.Tags, matching []string) uint64 { return tags.IDWithKeys(matching...) }

type withKeysTags func(tags models.Tags, matching []string) models.Tags

func includeKeysTags(tags models.Tags, matching []string) models.Tags {
	return tags.TagsWithKeys(matching)
}
func excludeKeysTags(tags models.Tags, matching []string) models.Tags {
	return tags.TagsWithoutKeys(matching)
}

// create the output, by tags,
// returns a list of seriesMeta for the combined series,
// and a list of [index lists].
// Input series that exist in an index list are mapped to the
// relevant index in the combined series meta.
func (c *CountNode) collectSeries(metas []block.SeriesMeta) ([][]int, []block.SeriesMeta) {
	type tagMatch struct {
		indices []int
		tags    models.Tags
	}

	var idFunc withKeysID
	var tagsFunc withKeysTags
	if c.without {
		idFunc = excludeKeysID
		tagsFunc = excludeKeysTags
	} else {
		idFunc = includeKeysID
		tagsFunc = includeKeysTags
	}

	tagMap := make(map[uint64]*tagMatch)
	for i, meta := range metas {
		id := idFunc(meta.Tags, c.matching)
		if val, ok := tagMap[id]; ok {
			val.indices = append(val.indices, i)
		} else {
			fmt.Println("Tags", meta.Tags)
			tagMap[id] = &tagMatch{
				indices: []int{i},
				tags:    tagsFunc(meta.Tags, c.matching),
			}
			fmt.Println("FuncTag", tagMap[id].tags)
		}
	}

	collectedMetas := make([]block.SeriesMeta, len(tagMap))
	collectedIndices := make([][]int, len(tagMap))

	i := 0
	for _, v := range tagMap {
		collectedMetas[i] = block.SeriesMeta{Tags: v.tags}
		collectedIndices[i] = v.indices
		i++
	}

	return collectedIndices, collectedMetas
}
