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

	"github.com/m3db/m3/src/x/ident"
)

func filterTagIterator(iter ident.TagIterator, filters []Filter) bool {
	if len(filters) == 0 || iter.Remaining() == 0 {
		return false
	}

	// NB: rewind iterator for re-use.
	defer iter.Rewind()
	for iter.Next() {
		tag := iter.Current()

		name := tag.Name.Bytes()
		value := tag.Value.Bytes()
		for _, f := range filters {
			if !bytes.Equal(name, f.Name) {
				continue
			}

			for _, filter := range f.Filters {
				if filter.Match(value) {
					return true
				}
			}
		}
	}

	return false
}
 