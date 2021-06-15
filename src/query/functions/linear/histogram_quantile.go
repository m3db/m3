// Copyright (c) 2019 Uber Technologies, Inc.
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

package linear

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/util"
)

const (
	// HistogramQuantileType calculates the quantile for histogram buckets.
	//
	// NB: each sample must contain a tag with a bucket name (given by tag
	// options) that denotes the upper bound of that bucket; series without this
	// tag are ignored.
	HistogramQuantileType = "histogram_quantile"
	initIndexBucketLength = 10
)

// NewHistogramQuantileOp creates a new histogram quantile operation.
func NewHistogramQuantileOp(
	args []interface{},
	opType string,
) (parser.Params, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf(
			"invalid number of args for histogram_quantile: %d", len(args))
	}

	if opType != HistogramQuantileType {
		return nil, fmt.Errorf("operator not supported: %s", opType)
	}

	q, ok := args[0].(float64)
	if !ok {
		return nil, fmt.Errorf("unable to cast to scalar argument: %v", args[0])
	}

	return newHistogramQuantileOp(q, opType), nil
}

// histogramQuantileOp stores required properties for histogram quantile ops.
type histogramQuantileOp struct {
	q      float64
	opType string
}

// OpType for the operator.
func (o histogramQuantileOp) OpType() string {
	return o.opType
}

// String representation.
func (o histogramQuantileOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node.
func (o histogramQuantileOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &histogramQuantileNode{
		op:         o,
		controller: controller,
	}
}

func newHistogramQuantileOp(
	q float64,
	opType string,
) histogramQuantileOp {
	return histogramQuantileOp{
		q:      q,
		opType: opType,
	}
}

type histogramQuantileNode struct {
	op         histogramQuantileOp
	controller *transform.Controller
}

type bucketValue struct {
	upperBound float64
	value      float64
}

type indexedBucket struct {
	upperBound float64
	idx        int
}

type indexedBuckets struct {
	buckets []indexedBucket
	tags    models.Tags
}

func (b indexedBuckets) Len() int { return len(b.buckets) }
func (b indexedBuckets) Swap(i, j int) {
	b.buckets[i], b.buckets[j] = b.buckets[j], b.buckets[i]
}

func (b indexedBuckets) Less(i, j int) bool {
	return b.buckets[i].upperBound < b.buckets[j].upperBound
}

type bucketedSeries map[string]indexedBuckets

type validSeriesBuckets []indexedBuckets

func (b validSeriesBuckets) Len() int      { return len(b) }
func (b validSeriesBuckets) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b validSeriesBuckets) Less(i, j int) bool {
	if len(b[i].buckets) == 0 {
		return false
	}

	if len(b[j].buckets) == 0 {
		return true
	}

	// An arbitrarily chosen sort that guarantees deterministic results.
	return b[i].buckets[0].idx < b[j].buckets[0].idx
}

func gatherSeriesToBuckets(metas []block.SeriesMeta) validSeriesBuckets {
	bucketsForID := make(bucketedSeries, initIndexBucketLength)
	for i, meta := range metas {
		tags := meta.Tags
		value, found := tags.Bucket()
		if !found {
			// this series does not have a bucket tag; drop it from the output.
			continue
		}

		bound, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			// invalid bounds value for the bucket; drop it from the output.
			continue
		}

		excludeTags := [][]byte{tags.Opts.MetricName(), tags.Opts.BucketName()}
		tagsWithoutKeys := tags.TagsWithoutKeys(excludeTags)
		id := string(tagsWithoutKeys.ID())
		newBucket := indexedBucket{
			upperBound: bound,
			idx:        i,
		}

		if buckets, found := bucketsForID[id]; !found {
			// add a single indexed bucket for this ID with the current index only.
			newBuckets := make([]indexedBucket, 0, initIndexBucketLength)
			newBuckets = append(newBuckets, newBucket)
			bucketsForID[id] = indexedBuckets{
				buckets: newBuckets,
				tags:    tagsWithoutKeys,
			}
		} else {
			buckets.buckets = append(buckets.buckets, newBucket)
			bucketsForID[id] = buckets
		}
	}

	return sanitizeBuckets(bucketsForID)
}

// sanitize sorts the bucket maps by upper bound, dropping any series which
// have less than two buckets, or any that do not have an upper bound of +Inf
func sanitizeBuckets(bucketMap bucketedSeries) validSeriesBuckets {
	validSeriesBuckets := make(validSeriesBuckets, 0, len(bucketMap))
	for _, buckets := range bucketMap {
		if len(buckets.buckets) < 2 {
			continue
		}

		sort.Sort(buckets)
		maxBound := buckets.buckets[len(buckets.buckets)-1].upperBound
		if !math.IsInf(maxBound, 1) {
			continue
		}

		validSeriesBuckets = append(validSeriesBuckets, buckets)
	}

	sort.Sort(validSeriesBuckets)
	return validSeriesBuckets
}

func bucketQuantile(q float64, buckets []bucketValue) float64 {
	// NB: some valid buckets may have been purged if the values at the current
	// step for that series are not present.
	if len(buckets) < 2 {
		return math.NaN()
	}

	// NB: similar situation here if the max bound bucket does not have a value
	// at this point, it is necessary to re-check.
	if !math.IsInf(buckets[len(buckets)-1].upperBound, 1) {
		return math.NaN()
	}

	rank := q * buckets[len(buckets)-1].value

	bucketIndex := sort.Search(len(buckets)-1, func(i int) bool {
		return buckets[i].value >= rank
	})

	if bucketIndex == len(buckets)-1 {
		return buckets[len(buckets)-2].upperBound
	}

	if bucketIndex == 0 && buckets[0].upperBound <= 0 {
		return buckets[0].upperBound
	}

	var (
		bucketStart float64
		bucketEnd   = buckets[bucketIndex].upperBound
		count       = buckets[bucketIndex].value
	)

	if bucketIndex > 0 {
		bucketStart = buckets[bucketIndex-1].upperBound
		count -= buckets[bucketIndex-1].value
		rank -= buckets[bucketIndex-1].value
	}

	return bucketStart + (bucketEnd-bucketStart)*rank/count
}

func (n *histogramQuantileNode) Params() parser.Params {
	return n.op
}

// Process the block
func (n *histogramQuantileNode) Process(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *histogramQuantileNode) ProcessBlock(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) (block.Block, error) {
	stepIter, err := b.StepIter()
	if err != nil {
		return nil, err
	}

	meta := b.Meta()
	seriesMetas := utils.FlattenMetadata(meta, stepIter.SeriesMeta())
	seriesBuckets := gatherSeriesToBuckets(seriesMetas)

	q := n.op.q
	if q < 0 || q > 1 {
		return processInvalidQuantile(queryCtx, q, seriesBuckets, meta, stepIter, n.controller)
	}

	return processValidQuantile(queryCtx, q, seriesBuckets, meta, stepIter, n.controller)
}

func setupBuilder(
	queryCtx *models.QueryContext,
	seriesBuckets validSeriesBuckets,
	meta block.Metadata,
	stepIter block.StepIter,
	controller *transform.Controller,
) (block.Builder, error) {
	metas := make([]block.SeriesMeta, 0, len(seriesBuckets))
	for _, v := range seriesBuckets {
		metas = append(metas, block.SeriesMeta{
			Tags: v.tags,
		})
	}

	builder, err := controller.BlockBuilder(queryCtx, meta, metas)
	if err != nil {
		return nil, err
	}

	if err = builder.AddCols(stepIter.StepCount()); err != nil {
		return nil, err
	}

	return builder, nil
}

// Enforce monotonicity for binary search to work.
// See https://github.com/prometheus/prometheus/commit/896f951e6846ce252d9d19fd4707a4110ceda5ee
func ensureMonotonic(bucketValues []bucketValue) {
	max := math.Inf(-1)
	for i := range bucketValues {
		switch {
		case bucketValues[i].value >= max:
			max = bucketValues[i].value
		case bucketValues[i].value < max:
			bucketValues[i].value = max
		}
	}
}

func processValidQuantile(
	queryCtx *models.QueryContext,
	q float64,
	seriesBuckets validSeriesBuckets,
	meta block.Metadata,
	stepIter block.StepIter,
	controller *transform.Controller,
) (block.Block, error) {
	builder, err := setupBuilder(queryCtx, seriesBuckets, meta, stepIter, controller)
	if err != nil {
		return nil, err
	}

	for index := 0; stepIter.Next(); index++ {
		step := stepIter.Current()
		values := step.Values()
		bucketValues := make([]bucketValue, 0, initIndexBucketLength)

		aggregatedValues := make([]float64, 0, len(seriesBuckets))
		for _, b := range seriesBuckets {
			buckets := b.buckets
			// clear previous bucket values.
			bucketValues = bucketValues[:0]
			for _, bucket := range buckets {
				// Only add non-NaN values to contention for the calculation.
				val := values[bucket.idx]
				if !math.IsNaN(val) {
					bucketValues = append(
						bucketValues, bucketValue{
							upperBound: bucket.upperBound,
							value:      val,
						},
					)
				}
			}

			ensureMonotonic(bucketValues)

			aggregatedValues = append(aggregatedValues, bucketQuantile(q, bucketValues))
		}

		if err := builder.AppendValues(index, aggregatedValues); err != nil {
			return nil, err
		}
	}

	if err = stepIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

func processInvalidQuantile(
	queryCtx *models.QueryContext,
	q float64,
	seriesBuckets validSeriesBuckets,
	meta block.Metadata,
	stepIter block.StepIter,
	controller *transform.Controller,
) (block.Block, error) {
	builder, err := setupBuilder(queryCtx, seriesBuckets, meta, stepIter, controller)
	if err != nil {
		return nil, err
	}

	// Set the values to an infinity of the appropriate sign; anything less than 0
	// becomes -Inf, anything greather than one becomes +Inf.
	sign := 1
	if q < 0 {
		sign = -1
	}

	setValue := math.Inf(sign)
	outValues := make([]float64, len(seriesBuckets))
	util.Memset(outValues, setValue)
	for index := 0; stepIter.Next(); index++ {
		if err := builder.AppendValues(index, outValues); err != nil {
			return nil, err
		}
	}

	if err = stepIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}
