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

package m3db

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

// Options describes the options for encoded block converters.
// These options are generally config-backed and don't usually change accross
// queries, unless certain query string parameters are present.
type Options interface {
	// SetSplitSeriesByBlock determines if the converter will split the series
	// by blocks, or if it will instead treat the entire series as a single block.
	//
	// NB: if a lookback duration greater than 0 has been set, the series will
	// always be treated as a single block.
	SetSplitSeriesByBlock(bool) Options
	// SplittingSeriesByBlock returns true iff lookback duration is 0, and the
	// options has not been forced to return a single block.
	SplittingSeriesByBlock() bool
	// SetLookbackDuration sets the lookback duration.
	SetLookbackDuration(time.Duration) Options
	// LookbackDuration returns the lookback duration.
	LookbackDuration() time.Duration
	// SetLookbackDuration sets the consolidation function for the converter.
	SetConsolidationFunc(consolidators.ConsolidationFunc) Options
	// LookbackDuration returns the consolidation function.
	ConsolidationFunc() consolidators.ConsolidationFunc
	// SetLookbackDuration sets the tag options for the converter.
	SetTagOptions(models.TagOptions) Options
	// TagOptions returns the tag options.
	TagOptions() models.TagOptions
	// SetIterAlloc sets the iterator allocator.
	SetIterAlloc(encoding.ReaderIteratorAllocate) Options
	// IterAlloc returns the reader iterator allocator.
	IterAlloc() encoding.ReaderIteratorAllocate
	// SetIteratorPools sets the iterator pools for the converter.
	SetIteratorPools(encoding.IteratorPools) Options
	// IteratorPools returns the iterator pools for the converter.
	IteratorPools() encoding.IteratorPools

	// Validate ensures that the given block options are valid.
	Validate() error
}
