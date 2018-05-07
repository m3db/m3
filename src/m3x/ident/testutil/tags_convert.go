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

package testutil

import (
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
)

// NewTagsFromTagIterator allocates tags for each tag for a tag iterator, this
// will copy bytes from the iterator.
func NewTagsFromTagIterator(iter ident.TagIterator) (ident.Tags, error) {
	defer iter.Close()

	var tags ident.Tags
	if tagsLen := iter.Remaining(); tagsLen > 0 {
		tags = make(ident.Tags, 0, tagsLen)
		for iter.Next() {
			curr := iter.Current()
			tagName := checked.NewBytes(append([]byte(nil), curr.Name.Bytes()...), nil)
			tagValue := checked.NewBytes(append([]byte(nil), curr.Value.Bytes()...), nil)
			tags = append(tags, ident.BinaryTag(tagName, tagValue))
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}
