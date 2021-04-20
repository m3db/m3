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

package aggregator

import (
	"testing"

	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestEntryPool(t *testing.T) {
	runtimeOpts := runtime.NewOptions()
	p := NewEntryPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() *Entry {
		return NewEntry(nil, runtimeOpts, newTestOptions())
	})

	// Retrieve an entry from the pool.
	entry := p.Get()
	lists := &metricLists{}
	entry.ResetSetData(&metricLists{}, runtimeOpts, newTestOptions())
	require.Equal(t, lists, entry.lists)

	// Put the entry back to pool.
	p.Put(entry)

	// Retrieve the entry and assert it's the same entry.
	entry = p.Get()
	require.Equal(t, lists, entry.lists)
}
