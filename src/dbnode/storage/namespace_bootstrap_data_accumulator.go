// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

type namespaceDataAccumulator struct {
	namespace    databaseNamespace
	needsRelease []lookup.OnReleaseReadWriteRef
}

func newDatabaseNamespaceDataAccumulator(
	namespace databaseNamespace,
) bootstrap.NamespaceDataAccumulator {
	return &namespaceDataAccumulator{
		namespace: namespace,
	}
}

func (a *namespaceDataAccumulator) CheckoutSeries(
	id ident.ID,
	tags ident.TagIterator,
) (bootstrap.CheckoutSeriesResult, error) {
	ref, err := a.namespace.SeriesReadWriteRef(id, tags)
	if err != nil {
		return bootstrap.CheckoutSeriesResult{}, err
	}

	a.needsRelease = append(a.needsRelease, ref.ReleaseReadWriteRef)
	return bootstrap.CheckoutSeriesResult{
		Series:      ref.Series,
		UniqueIndex: ref.UniqueIndex,
	}, nil
}

func (a *namespaceDataAccumulator) Release() {
	// Release all refs.
	for _, elem := range a.needsRelease {
		elem.OnReleaseReadWriteRef()
	}

	// Memset optimization for reset.
	for i := range a.needsRelease {
		a.needsRelease[i] = nil
	}
	a.needsRelease = a.needsRelease[:0]
}

func (a *namespaceDataAccumulator) Close() error {
	if n := len(a.needsRelease); n != 0 {
		// This is an error case, callers need to be
		// very explicit in the lifecycle to avoid releasing
		// refs at the right time so we return this as an error
		// since there is a code bug if this is called before
		// releasing all refs.
		a.Release()
		err := instrument.InvariantErrorf("closed with still open refs: %d", n)
		return err
	}
	return nil
}
