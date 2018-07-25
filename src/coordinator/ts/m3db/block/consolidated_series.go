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
	"github.com/m3db/m3db/src/coordinator/block"
)

// ConsolidatedBlock is a single series consolidated across different namespaces
// for a single block
type ConsolidatedBlock struct {
	Metadata          block.Metadata
	NSBlocks          []NSBlock
	consolidationFunc ConsolidationFunc // nolint
}

type consolidatedBlockIter struct {
	nsBlockIters []block.ValueIterator
}

// ConsolidationFunc determines how to consolidate across namespaces
type ConsolidationFunc func(existing, toAdd float64, count int) float64

// ConsolidatedBlocks contain all of the consolidated blocks for
// a single timeseries across namespaces.
type ConsolidatedBlocks []ConsolidatedBlock

func (c *consolidatedBlockIter) Current() float64 {
	values := make([]float64, 0, len(c.nsBlockIters))
	for _, iter := range c.nsBlockIters {
		dp := iter.Current()
		values = append(values, dp)
	}

	// todo(braskin): until we have consolidation
	return values[0]
}

// Next moves to the next item
func (c *consolidatedBlockIter) Next() bool {
	if len(c.nsBlockIters) == 0 {
		return false
	}

	for _, nsBlock := range c.nsBlockIters {
		if !nsBlock.Next() {
			return false
		}
	}

	return true
}

// Close closes the underlaying iterators
func (c *consolidatedBlockIter) Close() {
	// todo(braskin): implement this function
}
