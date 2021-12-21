// Copyright (c) 2021 Uber Technologies, Inc.
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

package storage

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/ident"
)

// retrieveWritableSeriesAndIncrementReaderWriterCountFn represents the function to retrieve series entry.
type retrieveWritableSeriesAndIncrementReaderWriterCountFn func(id ident.ID) (*Entry, error)

type seriesResolver struct {
	sync.RWMutex

	wg                                                    *sync.WaitGroup
	createdEntry                                          *Entry
	retrieveWritableSeriesAndIncrementReaderWriterCountFn retrieveWritableSeriesAndIncrementReaderWriterCountFn

	resolved    bool
	resolvedErr error
	entry       *Entry
}

// NewSeriesResolver creates new series ref resolver.
func NewSeriesResolver(
	wg *sync.WaitGroup,
	createdEntry *Entry,
	retrieveWritableSeriesAndIncrementReaderWriterCountFn retrieveWritableSeriesAndIncrementReaderWriterCountFn,
) bootstrap.SeriesRefResolver {
	return &seriesResolver{
		wg:           wg,
		createdEntry: createdEntry,
		retrieveWritableSeriesAndIncrementReaderWriterCountFn: retrieveWritableSeriesAndIncrementReaderWriterCountFn,
	}
}

func (r *seriesResolver) resolve() error {
	r.RLock()
	if r.resolved {
		resolvedResult := r.resolvedErr
		r.RUnlock()
		return resolvedResult
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()

	// Fast path: if we already resolved the result, just return it.
	if r.resolved {
		return r.resolvedErr
	}

	// Wait for the insertion.
	r.wg.Wait()

	// Retrieve the inserted entry.
	entry, err := r.retrieveWritableSeriesAndIncrementReaderWriterCountFn(r.createdEntry.ID)
	r.resolved = true
	if err != nil {
		r.resolvedErr = err
		return r.resolvedErr
	}

	if entry == nil {
		r.resolvedErr = fmt.Errorf("could not resolve: %s", r.createdEntry.ID)
		return r.resolvedErr
	}

	r.entry = entry
	return nil
}

func (r *seriesResolver) SeriesRef() (bootstrap.SeriesRef, error) {
	if err := r.resolve(); err != nil {
		return nil, err
	}
	return r.entry, nil
}

func (r *seriesResolver) ReleaseRef() {
	if r.createdEntry != nil {
		// We explicitly dec the originally created entry for the resolver since
		// that it is the one that was incremented before we took ownership of it,
		// this was done to make sure it was valid during insertion until we
		// operated on it.
		// If we got it back from the shard map and incremented the reader writer
		// count as well during that retrieval, then we'll again decrement it below.
		r.createdEntry.ReleaseRef()
		r.createdEntry = nil
	}
	if r.entry != nil {
		// To account for decrementing the increment that occurred when checking
		// out the series from the shard itself (which was incremented
		// the reader writer counter when we checked it out using by calling
		// "retrieveWritableSeriesAndIncrementReaderWriterCount").
		r.entry.DecrementReaderWriterCount()
		r.entry = nil
	}
}
