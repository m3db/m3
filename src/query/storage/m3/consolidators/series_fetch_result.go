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
	"github.com/m3db/m3/src/query/models"
)

// NewSeriesFetchResult creates a new series fetch result using the given
// iterators.
func NewSeriesFetchResult(
	iters encoding.SeriesIterators,
	tags []*models.Tags,
) SeriesFetchResult {
	if tags == nil {
		tags = make([]*models.Tags, iters.Len())
	}

	return SeriesFetchResult{
		seriesData: seriesData{
			seriesIterators: iters,
			tags:            tags,
		},
	}
}

// Verify verifies the fetch result is valid.
func (r SeriesFetchResult) Verify() error {
	if t, it := len(r.seriesData.tags), r.seriesData.seriesIterators.Len(); t != it {
		return fmt.Errorf("tag length %d does not match iterator length %d", t, it)
	}

	return nil
}

// Count returns the total number of contained series iterators.
func (r SeriesFetchResult) Count() int {
	return r.seriesData.seriesIterators.Len()
}

// Close closes the contained series iterators.
func (r SeriesFetchResult) Close() {
	r.seriesData.seriesIterators.Close()
}

// IterTagsAtIndex returns the tag iterator and tags at the given index.
func (r SeriesFetchResult) IterTagsAtIndex(
	idx int, tagOpts models.TagOptions,
) (encoding.SeriesIterator, models.Tags, error) {
	if idx < 0 || idx > len(r.seriesData.tags) {
		return nil, models.EmptyTags(), fmt.Errorf("series idx(%d) out of "+
			"bounds %d ", idx, len(r.seriesData.tags))
	}

	if r.seriesData.tags[idx] == nil {
		iter := r.seriesData.seriesIterators.Iters()[idx].Tags()
		tags, err := FromIdentTagIteratorToTags(iter, tagOpts)
		if err != nil {
			return nil, models.EmptyTags(), err
		}

		r.seriesData.tags[idx] = &tags
	}

	return r.seriesData.seriesIterators.Iters()[idx],
		*r.seriesData.tags[idx], nil
}
