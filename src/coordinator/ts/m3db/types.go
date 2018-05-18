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

package m3db

import (
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3x/ident"
)

// SeriesBlock is a placeholder until it is added to M3DB
type SeriesBlock struct {
	Start          time.Time
	End            time.Time
	SeriesIterator encoding.SeriesIterator
}

// SeriesBlocks is a placeholder until it is added to M3DB
type SeriesBlocks struct {
	ID        ident.ID
	Namespace string
	Blocks    []SeriesBlock
}

// MultiNamespaceSeries is a single timeseries for multiple namespaces
type MultiNamespaceSeries []SeriesBlocks

// ID enforces the same ID across namespaces
func (n MultiNamespaceSeries) ID() ident.ID { return n[0].ID }

// ConsolidatedNSBlock is a single block for a given timeseries and namespace
// which contains all of the necessary SeriesIterators so that consolidation can
// happen across namespaces
type ConsolidatedNSBlock struct {
	ID              ident.ID
	Namespace       string
	Start           time.Time
	End             time.Time
	SeriesIterators encoding.SeriesIterators
}

func (c ConsolidatedNSBlock) beyondBounds(consolidatedSeriesBlock ConsolidatedSeriesBlock) bool {
	if c.Start != consolidatedSeriesBlock.Start || c.End != consolidatedSeriesBlock.End {
		return false
	}
	return true
}

// ConsolidatedSeriesBlock is a single series consolidated across different namespaces
// for a single block
type ConsolidatedSeriesBlock struct {
	Start                time.Time
	End                  time.Time
	ConsolidatedNSBlocks []ConsolidatedNSBlock
	consolidationFunc    ConsolidationFunc // nolint
}

func (c ConsolidatedSeriesBlock) beyondBounds(multiSeriesBlock MultiSeriesBlock) bool {
	if c.Start != multiSeriesBlock.Start || c.End != multiSeriesBlock.End {
		return false
	}
	return true
}

// ConsolidationFunc determines how to consolidate across namespaces
type ConsolidationFunc func(existing, toAdd float64, count int) float64

// ConsolidatedSeriesBlocks contain all of the consolidated blocks for
// a single timeseries across namespaces.
// Each ConsolidatedBlockIterator will have the same size
type ConsolidatedSeriesBlocks []ConsolidatedSeriesBlock

// MultiSeriesBlock represents a vertically oriented block
type MultiSeriesBlock struct {
	Start  time.Time
	End    time.Time
	Blocks ConsolidatedSeriesBlocks
}

// MultiSeriesBlocks is a slice of MultiSeriesBlock
// todo(braskin): add close method on this to close each SeriesIterator
type MultiSeriesBlocks []MultiSeriesBlock
