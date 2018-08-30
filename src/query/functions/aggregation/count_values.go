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
	"strconv"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// CountValuesType counts the number of non nan elements with the same value
	CountValuesType = "count_values"
)

// NewCountValuesOp creates a new take operation
func NewCountValuesOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	if opType != CountValuesType {
		return baseOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return newCountValuesOp(params, opType), nil
}

// countValuesOp stores required properties for take ops
type countValuesOp struct {
	params   NodeParams
	opType   string
	takeFunc takeFunc
}

// OpType for the operator
func (o countValuesOp) OpType() string {
	return o.opType
}

// String representation
func (o countValuesOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o countValuesOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &countValuesNode{
		op:         o,
		controller: controller,
	}
}

func newCountValuesOp(params NodeParams, opType string) countValuesOp {
	return countValuesOp{
		params: params,
		opType: opType,
	}
}

type countValuesNode struct {
	op         countValuesOp
	controller *transform.Controller
}

type bucketColumn []float64

type bucketBlock struct {
	// metas        []block.SeriesMeta
	columns      []bucketColumn
	indexMapping map[float64]int
}

type tagValuePair struct {
	tag   models.Tag
	value float64
}

// Process the block
func (n *countValuesNode) Process(ID parser.NodeID, b block.Block) error {
	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	params := n.op.params
	meta := stepIter.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, metas := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		n.op.opType,
		seriesMetas,
	)

	stepCount := stepIter.StepCount()
	tempBlock := make([]bucketBlock, len(buckets))
	for i := range tempBlock {
		tempBlock[i].columns = make([]bucketColumn, stepCount)
		tempBlock[i].indexMapping = make(map[float64]int, 10)
	}

	for columnIndex := 0; stepIter.Next(); columnIndex++ {
		step, err := stepIter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		for bucketIndex, bucket := range buckets {
			currentBucketBlock := tempBlock[bucketIndex]
			// If not on the first column, generate appropriate number of rows full of NaNs
			if columnIndex > 0 {
				previousLen := len(currentBucketBlock.columns[columnIndex-1])
				currentBucketBlock.columns[columnIndex] = make(bucketColumn, previousLen)
				for i := 0; i < previousLen; i++ {
					currentBucketBlock.columns[columnIndex][i] = math.NaN()
				}
				if bucketIndex == 0 {
					// fmt.Println("********Filled up initial column index with", currentBucketBlock.columns[columnIndex])
				}
			}

			countedValues := countValuesFn(values, bucket)
			if bucketIndex == 0 {
				// fmt.Println("Values:", values, "bucket:", bucket, "counted", countedValues)
			}
			for distinctValue, count := range countedValues {
				currentBucketColumn := currentBucketBlock.columns[columnIndex]
				if bucketIndex == 0 {
					// fmt.Println("--current mapping", currentBucketBlock.indexMapping)
					// fmt.Println("      current columns", currentBucketBlock.columns[columnIndex])
				}
				if rowIndex, seen := currentBucketBlock.indexMapping[distinctValue]; seen {
					// This value has already been seen at rowIndex in a previous column
					// so add the current value to the appropriate row index.
					// fmt.Println("Seen", currentBucketBlock.indexMapping)
					// fmt.Println("Bucket index", bucketIndex, "col Index", columnIndex, "row Index", rowIndex, "col", currentBucketColumn)
					currentBucketBlock.columns[columnIndex][rowIndex] = count
				} else {
					// The column index needs to be created here already
					// Add the count to the end of the bucket column
					// fmt.Println("Adding", distinctValue, " to currentBucketBlock.column[", columnIndex, "]:", currentBucketBlock.columns[columnIndex])
					currentBucketBlock.columns[columnIndex] = append(currentBucketColumn, count)
					if bucketIndex == 0 {
						// fmt.Println("Added", distinctValue, " to currentBucketBlock.column[", columnIndex, "]:", currentBucketBlock.columns[columnIndex])
					}

					// Add the distinctValue to the indexMapping
					currentBucketBlock.indexMapping[distinctValue] = len(currentBucketColumn)
				}
			}
		}
	}

	numSeries := 0

	for _, bucketBlock := range tempBlock {
		numSeries += len(bucketBlock.indexMapping)
	}

	// Rebuild block metas in the expected order
	blockMetas := make([]block.SeriesMeta, numSeries)
	initialIndex := 0
	for bucketIndex, bucketBlock := range tempBlock {
		for k, v := range bucketBlock.indexMapping {
			blockMetas[v+initialIndex] = block.SeriesMeta{
				Name: n.op.OpType(),
				Tags: metas[bucketIndex].Tags.Clone().Add(models.Tags{{
					Name:  n.op.params.StringParameter,
					Value: strconv.FormatFloat(k, 'f', -1, 64),
				}}),
			}
		}

		initialIndex += len(bucketBlock.indexMapping)
	}

	// Dedupe common metadatas
	metaTags, flattenedMeta := utils.DedupeMetadata(blockMetas)
	meta.Tags = metaTags

	builder, err := n.controller.BlockBuilder(meta, flattenedMeta)
	if err != nil {
		return err
	}

	if err := builder.AddCols(stepCount); err != nil {
		return err
	}

	for columnIndex := 0; columnIndex < stepCount; columnIndex++ {
		for _, bucketBlock := range tempBlock {
			vals := []float64(bucketBlock.columns[columnIndex])
			valsToAdd := padValuesWithNaNs(vals, len(bucketBlock.indexMapping))
			builder.AppendValues(columnIndex, valsToAdd)
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return n.controller.Process(nextBlock)
}

// pads val with enough NaNs to match size
func padValuesWithNaNs(vals []float64, size int) []float64 {
	// 789
	// 456
	// 321
	//  0
	numPad := size - len(vals)
	for i := 0; i < numPad; i++ {
		vals = append(vals, math.NaN())
	}
	return vals
}

// count values takes a and, adding the count of distinct non nan values.
func countValuesFn(values []float64, buckets []int) map[float64]float64 {
	countedValues := make(map[float64]float64, len(buckets))
	for _, idx := range buckets {
		val := values[idx]
		if !math.IsNaN(val) {
			countedValues[val]++
		}
	}

	return countedValues
}
