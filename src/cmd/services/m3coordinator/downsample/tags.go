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

package downsample

import (
	"sort"

	"github.com/m3db/m3x/ident"
)

const (
	initAllocTagsSliceCapacity = 32
)

type tags struct {
	names    []string
	values   []string
	idx      int
	nameBuf  []byte
	valueBuf []byte
}

// Ensure tags implements TagIterator and sort Interface
var (
	_ ident.TagIterator = (*tags)(nil)
	_ sort.Interface    = (*tags)(nil)
)

func newTags() *tags {
	return &tags{
		names:  make([]string, 0, initAllocTagsSliceCapacity),
		values: make([]string, 0, initAllocTagsSliceCapacity),
		idx:    -1,
	}
}

func (t *tags) append(name, value string) {
	t.names = append(t.names, name)
	t.values = append(t.values, value)
}

func (t *tags) Len() int {
	return len(t.names)
}

func (t *tags) Swap(i, j int) {
	t.names[i], t.names[j] = t.names[j], t.names[i]
	t.values[i], t.values[j] = t.values[j], t.values[i]
}

func (t *tags) Less(i, j int) bool {
	return t.names[i] < t.names[j]
}

func (t *tags) Next() bool {
	hasNext := t.idx+1 < len(t.names)
	if hasNext {
		t.idx++
	}
	return hasNext
}

func (t *tags) CurrentIndex() int {
	if t.idx >= 0 {
		return t.idx
	}
	return 0
}

func (t *tags) Current() ident.Tag {
	t.nameBuf = append(t.nameBuf[:0], t.names[t.idx]...)
	t.valueBuf = append(t.valueBuf[:0], t.values[t.idx]...)
	return ident.Tag{
		Name:  ident.BytesID(t.nameBuf),
		Value: ident.BytesID(t.valueBuf),
	}
}

func (t *tags) Err() error {
	return nil
}

func (t *tags) Close() {
	// No-op
}

func (t *tags) Remaining() int {
	if t.idx < 0 {
		return t.Len()
	}
	return t.Len() - t.idx
}

func (t *tags) Duplicate() ident.TagIterator {
	return &tags{idx: -1, names: t.names, values: t.values}
}
