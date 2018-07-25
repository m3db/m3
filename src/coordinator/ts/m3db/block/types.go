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
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3x/ident"
)

// SeriesBlock contains the individual series iterators
type SeriesBlock struct {
	start          time.Time
	end            time.Time
	seriesIterator encoding.SeriesIterator
}

// SeriesBlocks contain information about the timeseries that gets returned from m3db.
// This includes meta data such as the ID, namespace and tags as well as the actual
// series iterators that contain the datapoints.
type SeriesBlocks struct {
	ID        ident.ID
	Namespace ident.ID
	Tags      ident.TagIterator
	Blocks    []SeriesBlock
}

// Close closes the series iterator in a SeriesBlock
func (s SeriesBlock) Close() {
	s.seriesIterator.Close()
}

// Close closes the underlaying series iterator within each SeriesBlock
// as well as the ID, Namespace, and Tags.
func (s SeriesBlocks) Close() {
	for _, seriesBlock := range s.Blocks {
		seriesBlock.Close()
	}

	s.Tags.Close()
	s.Namespace.Finalize()
	s.ID.Finalize()
}

// MultiNamespaceSeries is a single timeseries for multiple namespaces
type MultiNamespaceSeries []SeriesBlocks

// ID enforces the same ID across namespaces
func (n MultiNamespaceSeries) ID() ident.ID {
	if len(n) > 0 {
		return n[0].ID
	}
	return ident.StringID("")
}
