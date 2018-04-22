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
	currentTags ident.Tags
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
	// TODO(prateek): actually use a tags iterator instead of creating an un-required ident.Tags here
	return i.nsID, i.currentID, ident.NewTagSliceIterator(i.currentTags)
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
		i.currentTags.Finalize()
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
			continue
		}
		i.currentTags = append(i.currentTags,
			i.idPool.BinaryTag(i.wrapBytes(f.Name), i.wrapBytes(f.Value)))
	}

	if !idFound {
		return errInvalidResultMissingNamespace
	}

	return nil
}
