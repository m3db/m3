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
	"bytes"

	"github.com/m3db/m3/src/query/models"
)

type fetchResultMapWrapper struct {
	resultMap *fetchResultMap
}

func (w *fetchResultMapWrapper) len() int {
	return w.resultMap.Len()
}

func (w *fetchResultMapWrapper) list() []multiResultSeries {
	result := make([]multiResultSeries, 0, w.len())
	for _, results := range w.resultMap.Iter() {
		result = append(result, results.value)
	}

	return result
}

func (w *fetchResultMapWrapper) get(tags models.Tags) (multiResultSeries, bool) {
	return w.resultMap.Get(tags)
}

func (w *fetchResultMapWrapper) set(
	tags models.Tags, series multiResultSeries,
) {
	series.tags = tags
	w.resultMap.SetUnsafe(tags, series, fetchResultMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
}

// newFetchResultMap builds a MultiFetchResultMap, which is primarily used
// for checking for existence of particular ident.IDs.
func newFetchResultMap(size int) *fetchResultMapWrapper {
	return &fetchResultMapWrapper{
		resultMap: _fetchResultMapAlloc(_fetchResultMapOptions{
			hash: func(t models.Tags) fetchResultMapHash {
				return fetchResultMapHash(t.HashedID())
			},
			equals: func(x, y models.Tags) bool {
				// NB: IDs are calculated once, so any further calls to these
				// equals is a simple lookup.
				return bytes.Equal(x.ID(), y.ID())
			},
			initialSize: size,
		}),
	}
}
