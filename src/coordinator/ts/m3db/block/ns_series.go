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
	"fmt"
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
)

// NSBlock is a single block for a given timeseries and namespace
// which contains all of the necessary SeriesIterators so that consolidation can
// happen across namespaces
type NSBlock struct {
	ID              ident.ID
	Namespace       ident.ID
	Bounds          block.Bounds
	SeriesIterators encoding.SeriesIterators
}

type nsBlockIter struct {
	m3dbIters        []encoding.SeriesIterator
	bounds           block.Bounds
	seriesIndex, idx int
	lastDP           ts.Datapoint
}

// Next moves to the next item
func (c *nsBlockIter) Next() bool {
	c.idx++
	// NB(braskin): this is inclusive of the last step in the iterator
	fmt.Println("nsIter bounds: ", c.bounds)
	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil { // index is out of bounds
		return false
	}

	lastDP := c.lastDP
	// NB(braskin): check to make sure that the current index time is after the last
	// seen datapoint and Next() on the underlaying m3db iterator returns true
	// fmt.Println("NEXT: ", "idx time: ", indexTime, "last time: ", lastDP.Timestamp)
	for indexTime.After(lastDP.Timestamp) && c.nextIterator() {
		lastDP, _, _ = c.m3dbIters[c.seriesIndex].Current()
		c.lastDP = lastDP
		fmt.Println("updating current to: ", lastDP)
	}

	return true
}

func (c *nsBlockIter) nextIterator() bool {
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

// Current returns the float64 value for the current step
func (c *nsBlockIter) Current() float64 {
	lastDP := c.lastDP

	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil {
		return math.NaN()
	}

	// NB(braskin): if the last datapoint is after the current step, but before the (current step+1),
	// return that datapoint, otherwise return NaN
	// fmt.Println("CURRENT: ", lastDP.Value, "last time: ", lastDP.Timestamp, "idx time: ", indexTime, "idx + 1: ", indexTime.Add(c.bounds.StepSize))
	if !indexTime.After(lastDP.Timestamp) && indexTime.Add(c.bounds.StepSize).After(lastDP.Timestamp) {
		return lastDP.Value
	}

	return math.NaN()
}

// Close closes the underlaying iterators
func (c *nsBlockIter) Close() {
	// todo(braskin): implement this function
}
