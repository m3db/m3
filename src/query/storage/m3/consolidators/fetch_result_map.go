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
	xerrors "github.com/m3db/m3/src/x/errors"
)

type fetchResultMapWrapper struct {
	resultMap *fetchResultMap

	multiErr xerrors.MultiError
}

func (w *fetchResultMapWrapper) err() error {
	return w.multiErr.FinalError()
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

func (w *fetchResultMapWrapper) get(
	it encoding.SeriesIterator,
) (multiResultSeries, bool) {
	return w.resultMap.Get(it)
}

func (w *fetchResultMapWrapper) set(
	it encoding.SeriesIterator, series multiResultSeries,
) {
	// NB: no need to copy key; the
	w.resultMap.SetUnsafe(it, series, fetchResultMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
}

// newFetchResultMap builds a MultiFetchResultMap, which is primarily used
// for checking for existence of particular ident.IDs.
func newFetchResultMap(size int) *fetchResultMapWrapper {
	wrapper := &fetchResultMapWrapper{}

	addErr := func(msg string, err error) {
		err = fmt.Errorf("err, msg: %s, inner: %v", msg, err)
		wrapper.multiErr = wrapper.multiErr.Add(err)
	}

	resultMap := _fetchResultMapAlloc(_fetchResultMapOptions{
		hash: func(it encoding.SeriesIterator) fetchResultMapHash {
			hash, err := it.Tags().Hash()
			if err != nil {
				addErr("hashing iterator failed", err)
				return 0
			}

			return fetchResultMapHash(hash)
		},
		equals: func(x, y encoding.SeriesIterator) bool {
			// NB: fail fast on errors.
			err := wrapper.multiErr.FinalError()
			if err != nil {
				return false
			}

			// NB: succeed fast if possible with matching IDs.
			if x.ID() == y.ID() {
				return true
			}

			xTags, yTags := x.Tags(), y.Tags()
			// NB: fail fast when possible, as reading tags for equality is expensive.
			if xTags.Remaining() != yTags.Remaining() {
				return false
			}

			// NB: fail fast if hashes don't match; otherwise read all tags
			// sequentially to ensure they match.
			xHash, err := xTags.Hash()
			if err != nil {
				addErr("hashing x iterator failed", err)
				return false
			}

			yHash, err := yTags.Hash()
			if err != nil {
				addErr("hashing y iterator failed", err)
				return false
			}

			if xHash != yHash {
				return false
			}

			xTags, yTags = xTags.Duplicate(), yTags.Duplicate()
			defer func() {
				if err := xTags.Err(); err != nil {
					addErr("closing x iterator failed", err)
				}

				if err := yTags.Err(); err != nil {
					addErr("closing y iterator failed", err)
				}

				xTags.Close()
				yTags.Close()
			}()

			for xTags.Next() {
				// NB: still tags on xIterator; not equal.
				if !yTags.Next() {
					return false
				}

				if !xTags.Current().Equal(yTags.Current()) {
					return false
				}
			}

			// NB: still tags on yIterator; not equal.
			if yTags.Next() {
				return false
			}

			return true
		},
		initialSize: size,
	})

	wrapper.resultMap = resultMap
	return wrapper
}
