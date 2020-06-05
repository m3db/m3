// Copyright (c) 2020 Uber Technologies, Inc.
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

package consolidators

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

// NewSeriesFetchResult creates a new series fetch result using the given
// iterators.
func NewSeriesFetchResult(
	meta block.ResultMetadata,
	tags []*models.Tags,
	iters encoding.SeriesIterators,
) SeriesFetchResult {
	if tags == nil {
		tags = make([]*models.Tags, iters.Len())
	}

	return SeriesFetchResult{
		Metadata: meta,
		seriesData: seriesData{
			seriesIterators: iters,
			tags:            tags,
		},
	}
}

// NewEmptyFetchResult creates a new empty series fetch result.
func NewEmptyFetchResult(
	meta block.ResultMetadata,
) SeriesFetchResult {
	return SeriesFetchResult{
		Metadata: meta,
		seriesData: seriesData{
			seriesIterators: nil,
			tags:            []*models.Tags{},
		},
	}
}

// Verify verifies the fetch result is valid.
func (r SeriesFetchResult) Verify() error {
	tagLen := len(r.seriesData.tags)
	iterLen := r.seriesData.seriesIterators.Len()
	if tagLen != iterLen {
		return fmt.Errorf("tag length %d does not match iterator length %d",
			tagLen, iterLen)
	}

	return nil
}

// Count returns the total number of contained series iterators.
func (r SeriesFetchResult) Count() int {
	return r.seriesData.seriesIterators.Len()
}

// Close closes the contained series iterators.
func (r SeriesFetchResult) Close() {
	for _, it := range r.seriesData.seriesIterators.Iters() {
		it.Close()
	}
}

// IterTagsAtIndex returns the tag iterator and tags at the given index.
func (r SeriesFetchResult) IterTagsAtIndex(
	idx int, tagOpts models.TagOptions,
) (encoding.SeriesIterator, models.Tags, error) {
	if idx < 0 || idx > len(r.seriesData.tags) {
		return nil, models.EmptyTags(), fmt.Errorf("series idx(%d) out of "+
			"bounds %d ", idx, len(r.seriesData.tags))
	}

	iters := r.seriesData.seriesIterators.Iters()
	if r.seriesData.tags[idx] == nil {
		iter := iters[idx].Tags()
		tags, err := FromIdentTagIteratorToTags(iter, tagOpts)
		if err != nil {
			return nil, models.EmptyTags(), err
		}

		// TODO:
		iter.Rewind()
		r.seriesData.tags[idx] = &tags
	}

	return iters[idx],
		*r.seriesData.tags[idx], nil
}

// SeriesIterators returns the series iterators.
func (r SeriesFetchResult) SeriesIterators() []encoding.SeriesIterator {
	return r.seriesData.seriesIterators.Iters()
}
