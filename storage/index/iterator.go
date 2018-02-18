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

package index

import (
	"bytes"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
)

type idsIter struct {
	iter   segment.ResultsIter
	err    error
	idPool ident.Pool

	currentID   ident.ID
	currentNs   ident.ID
	currentTags ident.Tags
}

// NewIterator returns a new Iterator backed by
// a segment.ResultsIter.
func NewIterator(
	iter segment.ResultsIter,
	idPool ident.Pool,
) Iterator {
	i := &idsIter{
		iter:   iter,
		idPool: idPool,
	}
	return i
}

func (i *idsIter) Next() bool {
	// release any held resources
	i.release()

	// check underlying iterator
	next := i.iter.Next()
	if !next {
		i.err = i.iter.Err()
		return next
	}
	d, _ := i.iter.Current()
	i.parseAndStore(d)
	return true
}

func (i *idsIter) Current() (namespaceID ident.ID, seriesID ident.ID, tags ident.Tags) {
	return i.currentNs, i.currentID, i.currentTags
}

func (i *idsIter) Err() error {
	return i.err
}

func (i *idsIter) release() {
	if i.currentID != nil {
		i.idPool.Put(i.currentID)
		i.currentID = nil
	}
	if i.currentNs != nil {
		i.idPool.Put(i.currentNs)
		i.currentNs = nil
	}
	if i.currentTags != nil {
		// TODO(prateek): add ident.Pool method for - PutTags(Tags)
		for j := range i.currentTags {
			tag := i.currentTags[j]
			i.idPool.PutTag(tag)
			i.currentTags[j].Name = nil
			i.currentTags[j].Value = nil
		}
		i.currentTags = i.currentTags[:0]
	}
}

func (i *idsIter) parseAndStore(d doc.Document) {
	// TODO(prateek): add ident.Pool method for - Get([]byte) ID - do not assume ownership of input bytes
	i.currentID = ident.StringID(string(d.ID))
	nsFound := false
	for _, f := range d.Fields {
		if !nsFound && bytes.Equal(f.Name, ReservedFieldNameNamespace) {
			i.currentNs = ident.StringID(string(f.Value))
			nsFound = true
			continue
		}
		i.currentTags = append(i.currentTags,
			ident.StringTag(string(f.Name), string(f.Value)))
	}
}
