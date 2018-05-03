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

package result

import (
	"time"

	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

type indexBootstrapResult struct {
	results     IndexResults
	unfulfilled xtime.Ranges
}

// NewIndexBootstrapResult returns a new index bootstrap result.
func NewIndexBootstrapResult() IndexBootstrapResult {
	return &indexBootstrapResult{
		results:     make(IndexResults),
		unfulfilled: xtime.Ranges{},
	}
}

func (r *indexBootstrapResult) IndexResults() IndexResults {
	return r.results
}

func (r *indexBootstrapResult) Unfulfilled() xtime.Ranges {
	return r.unfulfilled
}

func (r *indexBootstrapResult) SetUnfulfilled(unfulfilled xtime.Ranges) {
	r.unfulfilled = unfulfilled
}

func (r *indexBootstrapResult) Add(block IndexBlock, unfulfilled xtime.Ranges) {
	r.results.Add(block)
	r.unfulfilled.AddRanges(unfulfilled)
}

// Add will add an index block to the collection, merging if one already exists.
func (r IndexResults) Add(block IndexBlock) {
	// Merge results
	blockStart := xtime.ToUnixNano(block.BlockStart())
	existing, ok := r[blockStart]
	if !ok {
		r[blockStart] = block
		return
	}
	r[blockStart] = existing.Merged(block.segments, block.notIndexed)
}

// NewIndexBlock returns a new bootstrap index block result.
func NewIndexBlock(
	blockStart time.Time,
	segments []segment.Segment,
	notIndexed map[string]SeriesNotIndexed,
) IndexBlock {
	return IndexBlock{
		blockStart: blockStart,
		segments:   segments,
		notIndexed: notIndexed,
	}
}

// BlockStart returns the block start.
func (b IndexBlock) BlockStart() time.Time {
	return b.blockStart
}

// Segments returns the segments.
func (b IndexBlock) Segments() []segment.Segment {
	return b.segments
}

// NotIndexed returns the not indexed series.
func (b IndexBlock) NotIndexed() map[string]SeriesNotIndexed {
	return b.notIndexed
}

// Merged returns a new merged index block, it mutates the current
// not indexed series map however to avoid allocating a new map.
func (b IndexBlock) Merged(
	segments []segment.Segment,
	notIndexed map[string]SeriesNotIndexed,
) IndexBlock {
	r := b
	r.segments = append(r.segments, segments...)
	for _, entry := range notIndexed {
		r.notIndexed[entry.ID().String()] = entry
	}
	return r
}

// NewSeriesNotIndexed returns a new not indexed series.
func NewSeriesNotIndexed(
	id ident.ID,
	tags ident.Tags,
) SeriesNotIndexed {
	return SeriesNotIndexed{id: id, tags: tags}
}

// ID returns the series ID.
func (s SeriesNotIndexed) ID() ident.ID {
	return s.id
}

// Tags returns the series tags.
func (s SeriesNotIndexed) Tags() ident.Tags {
	return s.tags
}
