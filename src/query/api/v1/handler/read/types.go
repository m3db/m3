// Copyright (c) 2021  Uber Technologies, Inc.
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

package read

import (
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
)

// TODO(nate): Delete the equivalents in the native package once prom path is refactored to
// stop using them.

// Result is a result from a read.
type Result struct {
	Series    []*ts.Series
	Meta      block.ResultMetadata
	BlockType block.BlockType
}

// RenderResultsOptions is a set of options for rendering the result.
type RenderResultsOptions struct {
	Start                   time.Time
	End                     time.Time
	ReturnedSeriesLimit     int
	ReturnedDatapointsLimit int
	KeepNaNs                bool
	Instant                 bool
}

// ResultIterator is an object for iterating through read query
// results.
type ResultIterator interface {
	// Next returns a boolean indicating whether there are more results in this
	// iterator.
	Next() bool

	// Current returns the current ts.Series at the head of the iterator.
	Current() *ts.Series

	// Metadata returns metadata from the read query.
	Metadata() block.ResultMetadata

	// BlockType returns the block type for the result.
	BlockType() block.BlockType

	// Reset starts the iterator over from the beginning so that it can be
	// re-used.
	Reset()

	// Size returns the total number of results in the iterator.
	Size() int
}

// ReturnedDataMetrics are metrics on the data returned from prom reads.
type ReturnedDataMetrics struct {
	FetchSeries     tally.Histogram
	FetchDatapoints tally.Histogram
}
