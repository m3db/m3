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

package block

import (
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
)

// ConsolidatedBlock is a single series consolidated across different namespaces
// for a single block
type ConsolidatedBlock struct {
	Metadata          block.Metadata
	NSBlocks          []NSBlock
	consolidationFunc ConsolidationFunc // nolint
}

type consolidatedBlockStepIter struct {
	nsBlockStepIters []block.ValueStepIterator
}

type consolidatedBlockSeriesIter struct {
	nsBlockSeriesIters []block.ValueSeriesIterator
}

// ConsolidationFunc determines how to consolidate across namespaces
type ConsolidationFunc func(existing, toAdd float64, count int) float64

// ConsolidatedBlocks contain all of the consolidated blocks for
// a single timeseries across namespaces.
type ConsolidatedBlocks []ConsolidatedBlock

func (c *consolidatedBlockStepIter) Current() float64 {
	values := make([]float64, 0, 1)
	for _, iter := range c.nsBlockStepIters {
		dp := iter.Current()
		values = append(values, dp)
	}

	if len(values) > 0 {
		// todo(braskin): until we have consolidation
		return values[0]
	}
	return math.NaN()
}

// Next moves to the next item
func (c *consolidatedBlockStepIter) Next() bool {
	if len(c.nsBlockStepIters) == 0 {
		return false
	}

	for _, nsBlock := range c.nsBlockStepIters {
		if !nsBlock.Next() {
			return false
		}
	}

	return true
}

// Close closes the underlaying iterators
func (c *consolidatedBlockStepIter) Close() {
	// todo(braskin): implement this function
}

// Current returns a slice of values for the current series
func (c *consolidatedBlockSeriesIter) Current() []float64 {
	values := make([][]float64, 0, 1)
	for _, iter := range c.nsBlockSeriesIters {
		dp := iter.Current()
		values = append(values, dp)
	}

	if len(values) > 0 {
		// todo(braskin): until we have consolidation
		return values[0]
	}
	return []float64{math.NaN()}
}

// Next moves to the next item
func (c *consolidatedBlockSeriesIter) Next() bool {
	if len(c.nsBlockSeriesIters) == 0 {
		return false
	}

	for _, nsBlock := range c.nsBlockSeriesIters {
		if !nsBlock.Next() {
			return false
		}
	}

	return true
}

// Close closes the underlaying iterators
func (c *consolidatedBlockSeriesIter) Close() {
	// todo(braskin): implement this function
}
