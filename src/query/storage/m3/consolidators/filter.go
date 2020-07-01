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
	"github.com/m3db/m3/src/x/ident"
)

func filterTagIterator(iter ident.TagIterator, filters models.Filters) bool {
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
 
type nameFilter []byte

func buildNameFilters(filters models.Filters) []nameFilter {
	nameFilters := make([]nameFilter, 0, len(filters))
	matchAll := false
	for _, filter := range filters {
		matchAll = false
		// Special case on a single wildcard matcher to drop an entire name.
		for _, filterValue := range filter.Filters {
			if filterValue.String() == ".*" {
				matchAll = true
				break
			}
		}

		if matchAll {
			nameFilters = append(nameFilters, nameFilter(filter.Name))
		}
	}

	return nameFilters
}

func filterNames(tags []CompletedTag, filters []nameFilter) []CompletedTag {
	if len(filters) == 0 || len(tags) == 0 {
		return tags
	}

	filteredTags := tags[:0]
	skip := false
	for _, tag := range tags {
		skip = false
		for _, f := range filters {
			if bytes.Equal(tag.Name, f) {
				skip = true
				break
			}
		}

		if !skip {
			filteredTags = append(filteredTags, tag)
		}
	}

	return filteredTags
}

func filterTags(tags []CompletedTag, filters models.Filters) []CompletedTag {
	if len(filters) == 0 || len(tags) == 0 {
		return tags
	}

	skip := false
	filteredTags := tags[:0]
	for _, tag := range tags {
		for _, f := range filters {
			if bytes.Equal(tag.Name, f.Name) {
				filteredValues := tag.Values[:0]
				for _, value := range tag.Values {
					skip = false
					for _, filter := range f.Filters {
						if filter.Match(value) {
							skip = true
							break
						}
					}

					if !skip {
						filteredValues = append(filteredValues, value)
					}
				}

				tag.Values = filteredValues
			}

			if len(tag.Values) == 0 {
				// NB: all values for this tag are invalid.
				continue
			}

			filteredTags = append(filteredTags, tag)
		}
	}

	return filteredTags
}
