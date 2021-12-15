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

	// fast path: if we already resolved the result, just return it.
	if r.resolved {
		return r.resolvedErr
	}

	r.wg.Wait()
	entry, err := r.retrieveWritableSeriesAndIncrementReaderWriterCountFn(r.createdEntry.Series.ID())
	r.resolved = true
	// Retrieve the inserted entry
	if err != nil {
		r.resolvedErr = err
		return r.resolvedErr
	}

	if entry == nil {
		r.resolvedErr = fmt.Errorf("could not resolve: %s", r.createdEntry.Series.ID())
		return r.resolvedErr
	}

	// NB: the responsibility for keeping the ref count on the entry is managed by the consume of this resolver.
	// On creation, the created entry is incremented and resolver.ReleaseRef() decrements that same entry. So
	// here we just want to retrieve the entry present on the shard without affecting the ref count.
	entry.DecrementReaderWriterCount()

	r.entry = entry
	return nil
}

func (r *seriesResolver) SeriesRef() (bootstrap.SeriesRef, error) {
	if err := r.resolve(); err != nil {
		return nil, err
	}
	return r.entry, nil
}

func (r *seriesResolver) ReleaseRef() error {
	// We explicitly dec the originally created entry for the resolver since that is the one that was incremented.
	// If the entry that won the race to the shard map was not this one, that means that some other resolver is
	// responsible for this decrement. This pattern ensures we don't have multiple resolver for the same series
	// calling decrement on the same entry more than once.
	return r.createdEntry.ReleaseRef()
}
