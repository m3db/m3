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

package doc

import (
	"fmt"

	"github.com/golang/mock/gomock"
)

// IteratorMatcher provides a matcher for an iterator over a collection of documents.
type IteratorMatcher interface {
	gomock.Matcher
}

// NewIteratorMatcher returns a new iterator matcher.
func NewIteratorMatcher(ensureOrdering bool, docs ...Document) IteratorMatcher {
	return &iterMatcher{
		ensureOrdering: ensureOrdering,
		docs:           docs,
	}
}

type iterMatcher struct {
	ensureOrdering bool
	docs           []Document
}

func toDocs(x interface{}) ([]Document, bool) {
	iter, ok := x.(Iterator)
	if !ok {
		return nil, false
	}
	var docs []Document
	for iter.Next() {
		d := iter.Current()
		var copied Document
		copied.ID = append([]byte(nil), d.ID...)
		for _, f := range d.Fields {
			copied.Fields = append(copied.Fields, Field{
				Name:  append([]byte(nil), f.Name...),
				Value: append([]byte(nil), f.Value...),
			})
		}
		docs = append(docs, copied)
	}
	if err := iter.Err(); err != nil {
		return nil, false
	}
	if err := iter.Close(); err != nil {
		return nil, false
	}
	return docs, true
}

func (im iterMatcher) Matches(x interface{}) bool {
	docs, ok := toDocs(x)
	if !ok {
		return false
	}
	if im.ensureOrdering {
		return im.matchExact(docs)
	}
	return im.matchAnyOrder(docs)
}

func (im iterMatcher) matchExact(docs []Document) bool {
	if len(im.docs) != len(docs) {
		return false
	}
	for i := range im.docs {
		if !im.docs[i].Equal(docs[i]) {
			return false
		}
	}
	return true
}

func (im iterMatcher) matchAnyOrder(docs []Document) bool {
	if len(im.docs) != len(docs) {
		return false
	}
	used := make([]bool, len(im.docs))
	for i := range im.docs {
		if used[i] {
			continue
		}
		found := false
		for j := range docs {
			if im.docs[i].Equal(docs[j]) {
				used[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for i := range used {
		if !used[i] {
			return false
		}
	}
	return true
}

func (im iterMatcher) String() string {
	return fmt.Sprintf("IteratorMatcher %+v", im.docs)
}
