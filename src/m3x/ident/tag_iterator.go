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

package ident

// NewTagIterator returns a new TagIterator over the given Tags.
func NewTagIterator(tags ...Tag) TagIterator {
	return NewTagSliceIterator(tags)
}

// NewTagSliceIterator returns a TagIterator over a slice.
func NewTagSliceIterator(tags Tags) TagIterator {
	iter := &tagSliceIter{
		backingSlice: tags,
		currentIdx:   -1,
	}
	return iter
}

type tagSliceIter struct {
	backingSlice Tags
	currentIdx   int
	currentTag   Tag
}

func (i *tagSliceIter) Next() bool {
	i.currentIdx++
	if i.currentIdx < len(i.backingSlice) {
		i.currentTag = i.backingSlice[i.currentIdx]
		return true
	}
	i.currentTag = Tag{}
	return false
}

func (i *tagSliceIter) Current() Tag {
	return i.currentTag
}

func (i *tagSliceIter) Err() error {
	return nil
}

func (i *tagSliceIter) Close() {
	i.backingSlice = nil
	i.currentIdx = 0
	i.currentTag = Tag{}
}

func (i *tagSliceIter) Remaining() int {
	if r := len(i.backingSlice) - 1 - i.currentIdx; r >= 0 {
		return r
	}
	return 0
}

func (i *tagSliceIter) Clone() TagIterator {
	return &tagSliceIter{
		backingSlice: i.backingSlice,
		currentIdx:   i.currentIdx,
		currentTag:   i.currentTag,
	}
}

// EmptyTagIterator returns an iterator over no tags.
var EmptyTagIterator TagIterator = &emptyTagIterator{}

type emptyTagIterator struct{}

func (e *emptyTagIterator) Next() bool         { return false }
func (e *emptyTagIterator) Current() Tag       { return Tag{} }
func (e *emptyTagIterator) Err() error         { return nil }
func (e *emptyTagIterator) Close()             {}
func (e *emptyTagIterator) Remaining() int     { return 0 }
func (e *emptyTagIterator) Clone() TagIterator { return e }
