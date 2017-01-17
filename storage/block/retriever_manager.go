// Copyright (c) 2016 Uber Technologies, Inc.
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

package block

import (
	"sync"

	"github.com/m3db/m3db/ts"
)

// NewDatabaseBlockRetrieverFn is a method for constructing
// new database block retrievers
type NewDatabaseBlockRetrieverFn func(
	namespace ts.ID,
) (DatabaseBlockRetriever, error)

// NewDatabaseBlockRetrieverManager creates a new manager
// for constructing and providing existing database block retrievers
func NewDatabaseBlockRetrieverManager(
	newDatabaseBlockRetrieverFn NewDatabaseBlockRetrieverFn,
) DatabaseBlockRetrieverManager {
	return &blockRetrieverManager{
		newRetrieverFn: newDatabaseBlockRetrieverFn,
		retrievers:     make(map[ts.Hash]DatabaseBlockRetriever),
	}
}

type blockRetrieverManager struct {
	sync.RWMutex
	newRetrieverFn NewDatabaseBlockRetrieverFn
	retrievers     map[ts.Hash]DatabaseBlockRetriever
}

func (m *blockRetrieverManager) Retriever(
	namespace ts.ID,
) (DatabaseBlockRetriever, error) {
	m.RLock()
	retriever, ok := m.retrievers[namespace.Hash()]
	m.RUnlock()

	if ok {
		return retriever, nil
	}

	m.Lock()
	defer m.Unlock()

	retriever, ok = m.retrievers[namespace.Hash()]
	if ok {
		return retriever, nil
	}

	var err error
	retriever, err = m.newRetrieverFn(namespace)
	if err != nil {
		return nil, err
	}

	m.retrievers[namespace.Hash()] = retriever
	return retriever, nil
}
