// Copyright (c) 2017 Uber Technologies, Inc.
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

package filters

import (
	"strings"

	"github.com/m3db/m3metrics/metric/id"
)

const (
	mockTagPairSeparator  = ","
	mockTagValueSeparator = "="
)

type mockTagPair struct {
	name  []byte
	value []byte
}

type mockSortedTagIterator struct {
	idx   int
	err   error
	pairs []mockTagPair
}

func tagsToPairs(tags []byte) []mockTagPair {
	tagPairs := strings.Split(string(tags), mockTagPairSeparator)
	var pairs []mockTagPair
	for _, pair := range tagPairs {
		p := strings.Split(pair, mockTagValueSeparator)
		pairs = append(pairs, mockTagPair{name: []byte(p[0]), value: []byte(p[1])})
	}
	return pairs
}

// NewMockSortedTagIterator creates a mock SortedTagIterator based on given ID.
func NewMockSortedTagIterator(tags []byte) id.SortedTagIterator {
	pairs := tagsToPairs(tags)
	return &mockSortedTagIterator{idx: -1, pairs: pairs}
}

func (it *mockSortedTagIterator) Reset(tags []byte) {
	it.idx = -1
	it.err = nil
	it.pairs = tagsToPairs(tags)
}

func (it *mockSortedTagIterator) Next() bool {
	if it.err != nil || it.idx >= len(it.pairs) {
		return false
	}
	it.idx++
	return it.err == nil && it.idx < len(it.pairs)
}

func (it *mockSortedTagIterator) Current() ([]byte, []byte) {
	return it.pairs[it.idx].name, it.pairs[it.idx].value
}

func (it *mockSortedTagIterator) Err() error {
	return it.err
}

func (it *mockSortedTagIterator) Close() {}
