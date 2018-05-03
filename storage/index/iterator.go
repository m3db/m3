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
	"github.com/m3db/m3db/storage/index/convert"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3x/ident"
)

var (
	idsIterZeroed = idsIter{}
)

type idsIter struct {
	copts convert.Opts

	nsID ident.ID
	iter doc.Iterator
	err  error

	currentID   ident.ID
	currentTags ident.TagIterator
}

// NewIterator returns a new Iterator backed by a doc.Iterator.
func NewIterator(
	nsID ident.ID,
	iter doc.Iterator,
	opts Options,
) Iterator {
	i := &idsIter{
		nsID: nsID,
		iter: iter,
		copts: convert.Opts{
			IdentPool:        opts.IdentifierPool(),
			CheckedBytesPool: opts.CheckedBytesPool(),
		},
	}
	return i
}

func (i *idsIter) Next() bool {
	// release any held resources
	i.releaseCurrent()

	if i.err != nil {
		return false
	}

	// check underlying iterator
	next := i.iter.Next()
	if !next {
		i.err = i.iter.Err()
		return next
	}

	d := i.iter.Current()
	id, tags, err := convert.ToMetric(d, i.copts)
	if err != nil {
		i.err = err
		return false
	}

	i.currentID = id
	i.currentTags = tags
	return true
}

func (i *idsIter) Current() (namespaceID ident.ID, seriesID ident.ID, tags ident.TagIterator) {
	return i.nsID, i.currentID, i.currentTags
}

func (i *idsIter) Err() error {
	return i.err
}

func (i *idsIter) Finalize() {
	i.releaseCurrent()
	*i = idsIterZeroed
	// TODO(prateek): implement idsIter pooling
}

func (i *idsIter) releaseCurrent() {
	if i.currentID != nil {
		i.currentID.Finalize()
		i.currentID = nil
	}
	if i.currentTags != nil {
		i.currentTags.Close()
		i.currentTags = nil
	}
	// NB(prateek): iterator doesn't own nsID so we don't finalize it here.
}
