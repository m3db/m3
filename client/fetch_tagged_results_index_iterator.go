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

package client

import (
	"github.com/m3db/m3db/serialize"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3x/ident"
)

// FOLLOWUP(prateek): add pooling for fetchTaggedResultsIndexIterator(s).
type fetchTaggedResultsIndexIterator struct {
	currentIdx int
	err        error
	pools      fetchTaggedPools

	current struct {
		nsID ident.ID
		tsID ident.ID
		tags serialize.TagDecoder
	}

	backing struct {
		nses [][]byte
		ids  [][]byte
		tags [][]byte
	}
}

// make the compiler ensure the concrete type `&fetchTaggedResultsIndexIterator{}` implements
// the `index.Iterator` interface.
var _ index.Iterator = &fetchTaggedResultsIndexIterator{}

func newFetchTaggedResultsIndexIterator(pools fetchTaggedPools) *fetchTaggedResultsIndexIterator {
	return &fetchTaggedResultsIndexIterator{
		currentIdx: -1,
		pools:      pools,
	}
}

func (i *fetchTaggedResultsIndexIterator) Next() bool {
	if i.err != nil || i.currentIdx >= len(i.backing.ids) {
		return false
	}
	i.release()
	i.currentIdx++
	if i.currentIdx >= len(i.backing.ids) {
		return false
	}

	dec := i.pools.TagDecoder().Get()
	wb := i.pools.CheckedBytesWrapper().Get(i.backing.tags[i.currentIdx])
	dec.Reset(wb)

	i.current.tsID = i.asIdent(i.backing.ids[i.currentIdx])
	i.current.nsID = i.asIdent(i.backing.nses[i.currentIdx])
	i.current.tags = dec
	return true
}

func (i *fetchTaggedResultsIndexIterator) addBacking(nsID, tsID, tags []byte) {
	i.backing.nses = append(i.backing.nses, nsID)
	i.backing.ids = append(i.backing.ids, tsID)
	i.backing.tags = append(i.backing.tags, tags)
}

func (i *fetchTaggedResultsIndexIterator) asIdent(b []byte) ident.ID {
	wb := i.pools.CheckedBytesWrapper().Get(b)
	return i.pools.ID().BinaryID(wb)
}

func (i *fetchTaggedResultsIndexIterator) Close() {
	i.release()
	i.backing.nses = nil
	i.backing.ids = nil
	i.backing.tags = nil
}

func (i *fetchTaggedResultsIndexIterator) release() {
	if id := i.current.nsID; id != nil {
		id.Finalize()
		i.current.nsID = nil
	}
	if id := i.current.tsID; id != nil {
		id.Finalize()
		i.current.tsID = nil
	}
	if decoder := i.current.tags; decoder != nil {
		decoder.Close()
		i.current.tags = nil
	}
}

func (i *fetchTaggedResultsIndexIterator) Current() (ident.ID, ident.ID, ident.TagIterator) {
	return i.current.nsID, i.current.tsID, i.current.tags
}

func (i *fetchTaggedResultsIndexIterator) Err() error {
	return i.err
}
