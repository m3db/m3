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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util"
)

const (
	// CountValuesType counts the number of non nan elements with the same value.
	CountValuesType = "count_values"
)

// NewCountValuesOp creates a new count values operation.
func NewCountValuesOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	if opType != CountValuesType {
		return baseOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return newCountValuesOp(params, opType), nil
}

// countValuesOp stores required properties for count values ops.
type countValuesOp struct {
	params NodeParams
	opType string
}

func (o countValuesOp) OpType() string {
	return o.opType
}

func (o countValuesOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

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

func (n *countValuesNode) Params() parser.Params {
	return n.op
}

// bucketColumn represents a column of times a particular value in a series has
// been seen. This may expand as more unique values are seen
type bucketColumn []float64

// bucketBlock is an abstraction for a set of series grouped by tags; count_values
// works on these groupings rather than the entire set of series.
type bucketBlock struct {
	// columnLength can expand as further columns are processed; used to initialize
	// the columns with empty values at each step
	columnLength int
	// columns indicates the number of times a value has been seen at a given step
	columns []bucketColumn
	// indexMapping maps any unique values seen to the appropriate column index
	indexMapping map[float64]int
}

// Processes all series in this block bucket at the current column.
func processBlockBucketAtColumn(
	currentBucketBlock *bucketBlock,
	values []float64,
	bucket []int,
	columnIndex int,
) {
	// Generate appropriate number of rows full of -1s that will later map to NaNs
	// unless updated with valid values
	currentColumnLength := currentBucketBlock.columnLength
	currentBucketBlock.columns[columnIndex] = make(bucketColumn, currentColumnLength)
	for i := 0; i < currentColumnLength; i++ {
		util.Memset(currentBucketBlock.columns[columnIndex], math.NaN())
	}

	countedValues := countValuesFn(values, bucket)
	for distinctValue, count := range countedValues {
		currentBucketColumn := currentBucketBlock.columns[columnIndex]
		if rowIndex, seen := currentBucketBlock.indexMapping[distinctValue]; seen {
			// This value has already been seen at rowIndex in a previous column
			// so add the current value to the appropriate row index.
			currentBucketColumn[rowIndex] = count
		} else {
			// The column index needs to be created here already
			// Add the count to the end of the bucket column
			currentBucketBlock.columns[columnIndex] = append(currentBucketColumn, count)

			// Add the distinctValue to the indexMapping
			currentBucketBlock.indexMapping[distinctValue] = len(currentBucketColumn)
		}
	}

	currentBucketBlock.columnLength = len(currentBucketBlock.columns[columnIndex])
}

// Process the block
func (n *countValuesNode) Process(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *countValuesNode) ProcessBlock(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) (block.Block, error) {
	meta := b.Meta()
	stepIter, err := b.StepIter()
	if err != nil {
		return nil, err
	}

	params := n.op.params
	labelName := params.StringParameter
	if !models.IsValid(labelName) {
		return nil, fmt.Errorf("invalid label name %q", labelName)
	}

	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	buckets, metas := utils.GroupSeries(
		params.MatchingTags,
		params.Without,
		[]byte(n.op.opType),
		seriesMetas,
	)

	stepCount := stepIter.StepCount()
	intermediateBlock := make([]bucketBlock, len(buckets))
	for i := range intermediateBlock {
		intermediateBlock[i].columns = make([]bucketColumn, stepCount)
		intermediateBlock[i].indexMapping = make(map[float64]int, len(buckets[i]))
	}

	for columnIndex := 0; stepIter.Next(); columnIndex++ {
		step := stepIter.Current()
		values := step.Values()
		for bucketIndex, bucket := range buckets {
			processBlockBucketAtColumn(
				&intermediateBlock[bucketIndex],
				values,
				bucket,
				columnIndex,
			)
		}
	}

	if err = stepIter.Err(); err != nil {
		return nil, err
	}

	numSeries := 0
	for _, bucketBlock := range intermediateBlock {
		numSeries += bucketBlock.columnLength
	}

	// Rebuild block metas in the expected order
	blockMetas := make([]block.SeriesMeta, numSeries)
	previousBucketBlockIndex := 0
	for bucketIndex, bucketBlock := range intermediateBlock {
		for k, v := range bucketBlock.indexMapping {
			// Add the metas of this bucketBlock right after the previous block
			blockMetas[v+previousBucketBlockIndex] = block.SeriesMeta{
				Name: []byte(n.op.opType),
				Tags: metas[bucketIndex].Tags.Clone().AddTag(models.Tag{
					Name:  []byte(labelName),
					Value: utils.FormatFloatToBytes(k),
				}),
			}
		}

		// NB: All metadatas for the intermediate block for this bucket have
		// been added to the combined block metas. The metadatas for the next
		// intermediate block should be added after these to maintain order
		previousBucketBlockIndex += bucketBlock.columnLength
	}

	// Dedupe common metadatas
	metaTags, flattenedMeta := utils.DedupeMetadata(blockMetas, meta.Tags.Opts)
	meta.Tags = metaTags

	builder, err := n.controller.BlockBuilder(queryCtx, meta, flattenedMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(stepCount); err != nil {
		return nil, err
	}

	for columnIndex := 0; columnIndex < stepCount; columnIndex++ {
		for _, bucketBlock := range intermediateBlock {
			valsToAdd := padValuesWithNaNs(
				bucketBlock.columns[columnIndex],
				len(bucketBlock.indexMapping),
			)
			if err := builder.AppendValues(columnIndex, valsToAdd); err != nil {
				return nil, err
			}
		}
	}

	return builder.Build(), nil
}

// pads vals with enough NaNs to match size
func padValuesWithNaNs(vals bucketColumn, size int) bucketColumn {
	numToPad := size - len(vals)
	for i := 0; i < numToPad; i++ {
		vals = append(vals, math.NaN())
	}

	return vals
}

// count values takes a value array and a bucket list, returns a map of
// distinct values to number of times the value was seen in this bucket.
// The distinct number returned here becomes the datapoint's value
func countValuesFn(values []float64, bucket []int) map[float64]float64 {
	countedValues := make(map[float64]float64, len(bucket))
	for _, idx := range bucket {
		val := values[idx]
		if !math.IsNaN(val) {
			countedValues[val]++
		}
	}

	return countedValues
}
