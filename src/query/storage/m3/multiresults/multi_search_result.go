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

package multiresults

import (
	"sync"

	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3x/errors"
)

type seen struct{}

type multiSearchResult struct {
	sync.Mutex
	result    *storage.SearchResults
	err       xerrors.MultiError
	dedupeMap *multiSearchResultMap
}

// NewMultiSearchResultBuilder returns a new multi search result builder
func NewMultiSearchResultBuilder() MultiSearchResultBuilder {
	return &multiSearchResult{}
}

func (r *multiSearchResult) Add(
	result *storage.SearchResults,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if r.result == nil {
		r.result = result
		return
	}

	// Need to dedupe
	if r.dedupeMap == nil {
		r.dedupeMap = newMultiSearchResultMap(multiSearchResultMapOptions{
			InitialSize: len(r.result.Metrics),
		})
		for _, s := range r.result.Metrics {
			r.dedupeMap.Set(s.ID, seen{})
		}
	}

	for _, s := range result.Metrics {
		id := s.ID
		_, exists := r.dedupeMap.Get(id)
		if exists {
			// Already exists
			continue
		}

		// Does not exist already, add result
		r.result.Metrics = append(r.result.Metrics, s)
		r.dedupeMap.Set(id, seen{})
	}
}

func (r *multiSearchResult) Build() (*storage.SearchResults, error) {
	if err := r.err.FinalError(); err != nil {
		return nil, err
	}

	return r.result, nil
}

func (r *multiSearchResult) Close() error { return nil }
