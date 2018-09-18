/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package storage

import (
	"errors"

	"github.com/m3db/m3/src/query/block"
)

type consolidatedBlock struct {
	unconsolidated    block.UnconsolidatedBlock
	consolidationFunc block.ConsolidationFunc
}

func (c *consolidatedBlock) Unconsolidated() (block.UnconsolidatedBlock, error) {
	return nil, errors.New("unconsolidated blocks are not supported")
}

func (c *consolidatedBlock) StepIter() (block.StepIter, error) {
	stepIter, err := c.unconsolidated.StepIter()
	if err != nil {
		return nil, err
	}

	return &consolidatedStepIter{
		unconsolidated:    stepIter,
		consolidationFunc: c.consolidationFunc,
	}, nil
}

func (c *consolidatedBlock) SeriesIter() (block.SeriesIter, error) {
	seriesIter, err := c.unconsolidated.SeriesIter()
	if err != nil {
		return nil, err
	}

	return &consolidatedSeriesIter{
		unconsolidated:    seriesIter,
		consolidationFunc: c.consolidationFunc,
	}, nil
}

func (c *consolidatedBlock) Close() error {
	return c.unconsolidated.Close()
}

type consolidatedStepIter struct {
	unconsolidated    block.UnconsolidatedStepIter
	consolidationFunc block.ConsolidationFunc
}

func (c *consolidatedStepIter) Next() bool {
	return c.unconsolidated.Next()
}

func (c *consolidatedStepIter) Close() {
	c.unconsolidated.Close()
}

func (c *consolidatedStepIter) Current() (block.Step, error) {
	step, err := c.unconsolidated.Current()
	if err != nil {
		return nil, err
	}

	stepValues := step.Values()
	consolidatedValues := make([]float64, len(stepValues))
	for i, singleSeriesValues := range stepValues {
		consolidatedValues[i] = c.consolidationFunc(singleSeriesValues)
	}

	return block.NewColStep(step.Time(), consolidatedValues), nil
}

func (c *consolidatedStepIter) StepCount() int {
	return c.unconsolidated.StepCount()
}

func (c *consolidatedStepIter) SeriesMeta() []block.SeriesMeta {
	return c.unconsolidated.SeriesMeta()
}

func (c *consolidatedStepIter) Meta() block.Metadata {
	return c.unconsolidated.Meta()
}

type consolidatedSeriesIter struct {
	unconsolidated    block.UnconsolidatedSeriesIter
	consolidationFunc block.ConsolidationFunc
}

func (c *consolidatedSeriesIter) Next() bool {
	return c.unconsolidated.Next()
}

func (c *consolidatedSeriesIter) Close() {
	c.unconsolidated.Close()
}

func (c *consolidatedSeriesIter) Current() (block.Series, error) {
	series, err := c.unconsolidated.Current()
	if err != nil {
		return block.Series{}, err
	}

	return series.Consolidated(c.consolidationFunc), nil
}

func (c *consolidatedSeriesIter) SeriesCount() int {
	return c.unconsolidated.SeriesCount()
}

func (c *consolidatedSeriesIter) SeriesMeta() []block.SeriesMeta {
	return c.unconsolidated.SeriesMeta()
}

func (c *consolidatedSeriesIter) Meta() block.Metadata {
	return c.unconsolidated.Meta()
}
