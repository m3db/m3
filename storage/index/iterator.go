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
	"errors"

	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/resource"
)

var (
	errInvalidResultMissingNamespace = errors.New("corrupt data, unable to extract namespace")
)

type idsIter struct {
	nsID        ident.ID
	iter        doc.Iterator
	err         error
	idPool      ident.Pool
	wrapperPool xpool.CheckedBytesWrapperPool
	finalizerFn resource.FinalizerFn

	currentID   ident.ID
	currentTags ident.TagIterator
}

// NewIterator returns a new Iterator backed by a doc.Iterator.
func NewIterator(
	nsID ident.ID,
	iter doc.Iterator,
	opts Options,
	finalizerFn resource.FinalizerFn,
) Iterator {
	i := &idsIter{
		nsID:        nsID,
		iter:        iter,
		idPool:      opts.IdentifierPool(),
		wrapperPool: opts.CheckedBytesWrapperPool(),
		finalizerFn: finalizerFn,
	}
	return i
}

func (i *idsIter) Next() bool {
	// release any held resources
	i.release()

	if i.err != nil {
		return false
	}

	// check underlying iterator
	next := i.iter.Next()
	if !next {
		i.err = i.iter.Err()
		i.finalizerFn()
		return next
	}

	d := i.iter.Current()
	err := i.parseAndStore(d)
	if err != nil {
		i.err = err
		i.finalizerFn()
		return false
	}

	return true
}

func (i *idsIter) Current() (namespaceID ident.ID, seriesID ident.ID, tags ident.TagIterator) {
	return i.nsID, i.currentID, i.currentTags
}

func (i *idsIter) Err() error {
	return i.err
}

func (i *idsIter) release() {
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

func (i *idsIter) wrapBytes(bytes []byte) checked.Bytes {
	return i.wrapperPool.Get(bytes)
}

func (i *idsIter) parseAndStore(d doc.Document) error {
	idFound := false
	for _, f := range d.Fields {
		if !idFound && bytes.Equal(f.Name, ReservedFieldNameID) {
			i.currentID = i.idPool.BinaryID(i.wrapBytes(f.Value))
			idFound = true
			break
		}
	}
	if !idFound {
		return errInvalidResultMissingNamespace
	}
	i.currentTags = newTagIter(d, i.idPool, i.wrapperPool)
	return nil
}

// tagIter exposes an ident.TagIterator interface over a doc.Document.
// NB: it skips any field marked ReservedFieldNameID.
type tagIter struct {
	d doc.Document

	err           error
	done          bool
	haveRunIntoID bool
	currentIdx    int
	currentTag    ident.Tag

	idPool      ident.Pool
	wrapperPool xpool.CheckedBytesWrapperPool
}

func newTagIter(d doc.Document, id ident.Pool, wrapper xpool.CheckedBytesWrapperPool) ident.TagIterator {
	return &tagIter{
		d:           d,
		currentIdx:  -1,
		idPool:      id,
		wrapperPool: wrapper,
	}
}

func (t *tagIter) Next() bool {
	if t.err != nil || t.done {
		return false
	}
	hasNext := t.parseNext()
	if !hasNext {
		t.done = true
	}
	return hasNext
}

func (t *tagIter) parseNext() (hasNext bool) {
	t.releaseCurrent()
	t.currentIdx++
	// early terminate if we know there's no more fieldsj
	if t.currentIdx >= len(t.d.Fields) {
		return false
	}
	// if there are fields, we have to ensure the next field
	// is not using the reserved ID fieldname
	next := t.d.Fields[t.currentIdx]
	if bytes.Equal(ReservedFieldNameID, next.Name) {
		t.haveRunIntoID = true
		return t.parseNext()
	}
	// otherwise, we're good.
	t.currentTag = t.idPool.BinaryTag(
		t.wrapBytes(next.Name), t.wrapBytes(next.Value))
	return true
}

func (t *tagIter) releaseCurrent() {
	if t.currentTag.Name != nil {
		t.currentTag.Name.Finalize()
		t.currentTag.Name = nil
	}
	if t.currentTag.Value != nil {
		t.currentTag.Value.Finalize()
		t.currentTag.Value = nil
	}
}

func (t *tagIter) Current() ident.Tag {
	return t.currentTag
}

func (t *tagIter) Err() error {
	return t.err
}

func (t *tagIter) Close() {
	t.releaseCurrent()
	t.done = true
}

func (t *tagIter) Remaining() int {
	l := len(t.d.Fields) - t.currentIdx - 1
	if !t.haveRunIntoID {
		l--
	}
	return l
}

func (t *tagIter) Duplicate() ident.TagIterator {
	var dupe = *t
	if t.currentTag.Name != nil {
		dupe.currentTag = t.idPool.CloneTag(t.currentTag)
	}
	return &dupe
}

func (t *tagIter) wrapBytes(bytes []byte) checked.Bytes {
	return t.wrapperPool.Get(bytes)
}

var _ ident.TagIterator = &tagIter{}
