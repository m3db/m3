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
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
)

// ConsolidatedNSBlock is a single block for a given timeseries and namespace
// which contains all of the necessary SeriesIterators so that consolidation can
// happen across namespaces
type ConsolidatedNSBlock struct {
	ID              ident.ID
	Namespace       ident.ID
	Bounds          block.Bounds
	SeriesIterators encoding.SeriesIterators
}

type consolidatedNSBlockIter struct {
	m3dbIters        []encoding.SeriesIterator
	bounds           block.Bounds
	seriesIndex, idx int
	lastDP           ts.Datapoint
}

type consolidatedNSBlockIters []*consolidatedNSBlockIter

// Next moves to the next item
func (c *consolidatedNSBlockIter) Next() bool {
	c.idx++
	// NB(braskin): this is inclusive of the last step in the iterator
	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil {
		return false
	}

	lastDP := c.lastDP
	// NB(braskin): check to make sure that the current index time is after the last
	// seen datapoint and Next() on the underlaying m3db iterator returns true
	for indexTime.After(lastDP.Timestamp) && c.nextIterator() {
		lastDP, _, _ = c.m3dbIters[c.seriesIndex].Current()
		c.lastDP = lastDP
	}

	return true
}

func (c *consolidatedNSBlockIter) nextIterator() bool {
	// todo(braskin): check bounds as well
	if len(c.m3dbIters) == 0 {
		return false
	}

	for c.seriesIndex < len(c.m3dbIters) {
		if c.m3dbIters[c.seriesIndex].Next() {
			return true
		}
		c.seriesIndex++
	}

	return false
}

// Current returns the float64 value for that step
func (c *consolidatedNSBlockIter) Current() float64 {
	lastDP := c.lastDP
	// NB(braskin): if the last datapoint is after the current step, but before the (current step+1),
	// return that datapoint, otherwise return NaN
	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil {
		return math.NaN()
	}
	if !indexTime.After(lastDP.Timestamp) && indexTime.Add(c.bounds.StepSize).After(lastDP.Timestamp) {
		return lastDP.Value
	}

	return math.NaN()
}

// Close closes the underlaying iterators
func (c *consolidatedNSBlockIter) Close() {
	// todo(braskin): implement this function
}
