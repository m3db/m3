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
	"errors"
	"sync"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/ident"
)

var (
	errAlreadyClosed = errors.New("accumulator already closed")
)

type namespaceDataAccumulator struct {
	sync.RWMutex
	closed       bool
	namespace    databaseNamespace
	needsRelease []bootstrap.SeriesRefResolver
}

// NewDatabaseNamespaceDataAccumulator creates a data accumulator for
// the namespace.
func NewDatabaseNamespaceDataAccumulator(
	namespace databaseNamespace,
) bootstrap.NamespaceDataAccumulator {
	return &namespaceDataAccumulator{
		namespace: namespace,
	}
}

func (a *namespaceDataAccumulator) CheckoutSeriesWithoutLock(
	shardID uint32,
	id ident.ID,
	tags ident.TagIterator,
) (bootstrap.CheckoutSeriesResult, bool, error) {
	resolver, owned, err := a.namespace.SeriesRefResolver(shardID, id, tags)
	if err != nil {
		return bootstrap.CheckoutSeriesResult{}, owned, err
	}

	a.needsRelease = append(a.needsRelease, resolver)
	return bootstrap.CheckoutSeriesResult{
		Resolver: resolver,
		Shard:    shardID,
	}, true, nil
}

func (a *namespaceDataAccumulator) CheckoutSeriesWithLock(
	shardID uint32,
	id ident.ID,
	tags ident.TagIterator,
) (bootstrap.CheckoutSeriesResult, bool, error) {
	a.Lock()
	result, owned, err := a.CheckoutSeriesWithoutLock(shardID, id, tags)
	a.Unlock()
	return result, owned, err
}

func (a *namespaceDataAccumulator) Close() error {
	a.Lock()
	defer a.Unlock()

	if a.closed {
		return errAlreadyClosed
	}

	// Release all refs.
	for _, elem := range a.needsRelease {
		elem.ReleaseRef()
	}

	a.closed = true

	// Memset optimization for reset.
	for i := range a.needsRelease {
		a.needsRelease[i] = nil
	}
	a.needsRelease = a.needsRelease[:0]

	return nil
}
