// Copyright (c) 2020 Uber Technologies, Inc.
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
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	xtime "github.com/m3db/m3/src/x/time"
)

type namespaceIndexer struct {
	index NamespaceIndex
}

func newDatabaseNamespaceIndexer(
	index NamespaceIndex,
) bootstrap.NamespaceIndexer {
	return &namespaceIndexer{
		index: index,
	}
}

func (i *namespaceIndexer) WritePending(
	writes []writes.PendingIndexInsert,
) error {
	return i.index.WritePending(writes)
}

func (i *namespaceIndexer) BlockStartForWriteTime(ts time.Time) xtime.UnixNano {
	return i.index.BlockStartForWriteTime(ts)
}
