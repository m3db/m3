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

package temporal

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/opentracing"
	xtime "github.com/m3db/m3/src/x/time"
)

var emptyOp = baseOp{}

type iterationBounds struct {
	start xtime.UnixNano
	end   xtime.UnixNano
}

// makeProcessor is a way to create a transform.
type makeProcessor interface {
	// initialize initializes the processor.
	initialize(
		duration time.Duration,
		controller *transform.Controller,
		opts transform.Options,
	) processor
}

// processor is implemented by the underlying transforms.
type processor interface {
	process(valueBuffer ts.Datapoints, iterationBounds iterationBounds) float64
}

// baseOp stores required properties for logical operations.
type baseOp struct {
	operatorType string
	duration     time.Duration
	processorFn  makeProcessor
}

func newBaseOp(
	duration time.Duration,
	operatorType string,
	processorFn makeProcessor,
) (baseOp, error) {
	return baseOp{
		operatorType: operatorType,
		processorFn:  processorFn,
		duration:     duration,
	}, nil
}

func (o baseOp) OpType() string {
	return o.operatorType
}

func (o baseOp) String() string {
	return fmt.Sprintf("type: %s, duration: %v", o.OpType(), o.duration)
}

// Node creates an execution node.
func (o baseOp) Node(
	controller *transform.Controller,
	opts transform.Options,
) transform.OpNode {
	return &baseNode{
		controller:    controller,
		op:            o,
		processor:     o.processorFn.initialize(o.duration, controller, opts),
		transformOpts: opts,
	}
}

// baseNode is an execution node.
type baseNode struct {
	// controller uses an interface here so we can mock it out in tests.
	// TODO: use an exported interface everywhere instead of *transform.Controller.
	// https://github.com/m3db/m3/issues/1430
	controller    controller
	op            baseOp
	processor     processor
	transformOpts transform.Options
}

func (c *baseNode) Process(
	queryCtx *models.QueryContext,
	id parser.NodeID,
	b block.Block,
) error {
	sp, _ := opentracing.StartSpanFromContext(queryCtx.Ctx, c.op.OpType())
	defer sp.Finish()

	meta := b.Meta()
	bounds := meta.Bounds
	if bounds.Duration == 0 {
		return fmt.Errorf("bound duration cannot be 0, bounds: %v", bounds)
	}

	seriesIter, err := b.SeriesIter()
	if err != nil {
		return err
	}

	// rename series to exclude their __name__ tag as part of function processing.
	resultSeriesMeta := make([]block.SeriesMeta, 0, len(seriesIter.SeriesMeta()))
	for _, m := range seriesIter.SeriesMeta() {
		tags := m.Tags.WithoutName()
		resultSeriesMeta = append(resultSeriesMeta, block.SeriesMeta{
			Name: tags.ID(),
			Tags: tags,
		})
	}

	builder, err := c.controller.BlockBuilder(queryCtx, meta, resultSeriesMeta)
	if err != nil {
		return err
	}

	steps := bounds.Steps()
	if err := builder.AddCols(steps); err != nil {
		return err
	}

	m := blockMeta{
		end:         xtime.ToUnixNano(bounds.Start),
		seriesMeta:  resultSeriesMeta,
		aggDuration: xtime.UnixNano(c.op.duration),
		stepSize:    xtime.UnixNano(bounds.StepSize),
		steps:       steps,
	}

	concurrency := runtime.NumCPU()
	batches, err := b.MultiSeriesIter(concurrency)
	if err != nil {
		// NB: If the unconsolidated block does not support multi series iteration,
		// fallback to processing series one by one.
		singleProcess(seriesIter, builder, m, c.processor)
	} else {
		batchProcess(batches, builder, m, c.processor)
	}

	// NB: safe to close the block here.
	if err := b.Close(); err != nil {
		return err
	}

	bl := builder.Build()
	defer bl.Close()
	return c.controller.Process(queryCtx, bl)
}

type blockMeta struct {
	end         xtime.UnixNano
	aggDuration xtime.UnixNano
	stepSize    xtime.UnixNano
	steps       int
	seriesMeta  []block.SeriesMeta
}

func batchProcess(
	iterBatches []block.SeriesIterBatch,
	builder block.Builder,
	m blockMeta,
	p processor,
) error {
	var (
		metas = m.seriesMeta

		mu       sync.Mutex
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		idx      int
	)

	builder.PopulateColumns(len(metas))
	for _, batch := range iterBatches {
		wg.Add(1)
		// capture loop variables
		loopIndex := idx
		batch := batch
		idx = idx + batch.Size
		go func() {
			if err := buildBlockBatch(loopIndex, batch.Iter,
				builder, m, p, &mu); err != nil {
				mu.Lock()
				// NB: this no-ops if the error is nil.
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}

			wg.Done()
		}()
	}

	wg.Wait()
	return multiErr.FinalError()
}

func buildBlockBatch(
	idx int,
	iter block.SeriesIter,
	builder block.Builder,
	blockMeta blockMeta,
	processor processor,
	mu *sync.Mutex,
) error {
	values := make([]float64, 0, blockMeta.steps)
	for iter.Next() {
		var (
			newVal float64
			init   = 0
			end    = blockMeta.end
			start  = end - blockMeta.aggDuration
			step   = blockMeta.stepSize

			series     = iter.Current()
			datapoints = series.Datapoints()
		)

		values = values[:0]
		for i := 0; i < blockMeta.steps; i++ {
			iterBounds := iterationBounds{
				start: start,
				end:   end,
			}

			l, r, b := getIndices(datapoints, start, end, init)
			if !b {
				newVal = processor.process(ts.Datapoints{}, iterBounds)
			} else {
				init = l
				newVal = processor.process(datapoints[l:r], iterBounds)
			}

			values = append(values, newVal)
			start += step
			end += step
		}

		mu.Lock()
		// NB: this sets the values internally, so no need to worry about keeping
		// a reference to underlying `values`.
		err := builder.SetRow(idx, values, blockMeta.seriesMeta[idx])
		mu.Unlock()
		idx++
		if err != nil {
			return err
		}
	}

	return iter.Err()
}

func singleProcess(
	seriesIter block.SeriesIter,
	builder block.Builder,
	m blockMeta,
	p processor,
) error {
	for seriesIter.Next() {
		var (
			newVal float64
			init   = 0
			end    = m.end
			start  = end - m.aggDuration
			step   = m.stepSize

			series     = seriesIter.Current()
			datapoints = series.Datapoints()
		)

		for i := 0; i < m.steps; i++ {
			iterBounds := iterationBounds{
				start: start,
				end:   end,
			}

			l, r, b := getIndices(datapoints, start, end, init)
			if !b {
				newVal = p.process(ts.Datapoints{}, iterBounds)
			} else {
				init = l
				newVal = p.process(datapoints[l:r], iterBounds)
			}

			if err := builder.AppendValue(i, newVal); err != nil {
				return err
			}

			start += step
			end += step
		}
	}

	return seriesIter.Err()
}

// getIndices returns the index of the points on the left and the right of the
// datapoint list given a starting index, as well as a boolean indicating if
// the returned indices are valid.
//
// NB: return values from getIndices should be used as subslice indices rather
// than direct index accesses, as that may cause panics when reaching the end of
// the datapoint list.
func getIndices(
	dps []ts.Datapoint,
	lBound xtime.UnixNano,
	rBound xtime.UnixNano,
	init int,
) (int, int, bool) {
	if init >= len(dps) || init < 0 {
		return -1, -1, false
	}

	var (
		l, r      = init, -1
		leftBound = false
	)

	for i, dp := range dps[init:] {
		ts := xtime.ToUnixNano(dp.Timestamp)
		if !leftBound {
			// Trying to set left bound.
			if ts < lBound {
				// data point before 0.
				continue
			}

			leftBound = true
			l = i
		}

		if ts <= rBound {
			continue
		}

		r = i
		break
	}

	if r == -1 {
		r = len(dps)
	} else {
		r = r + init
	}

	if leftBound {
		l = l + init
	} else {
		// if left bound was not found, there are no valid candidate points here.
		return l, r, false
	}

	return l, r, true
}
