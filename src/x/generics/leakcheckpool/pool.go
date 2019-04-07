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

package leakcheckpool

import (
	"fmt"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/mauricelam/genny/generic"
	"github.com/stretchr/testify/require"
)

// elemType is the generic type for use with the debug pool.
type elemType generic.Type

// elemTypeEqualsFn allows users to override equality checks
// for `elemType` instances.
type elemTypeEqualsFn func(a, b elemType) bool

// elemTypeGetHookFn allows users to override properties on items
// retrieved from the backing pools before returning in the Get()
// path.
type elemTypeGetHookFn func(elemType) elemType

// elemTypePool is the backing pool wrapped by the debug pool.
type elemTypePool interface {
	generic.Type

	Init()
	Get() elemType
	Put(elemType)
}

// leakcheckElemTypePoolOpts allows users to override default behaviour.
type leakcheckElemTypePoolOpts struct {
	DisallowUntrackedPuts bool
	EqualsFn              elemTypeEqualsFn
	GetHookFn             elemTypeGetHookFn
}

// newLeakcheckElemTypePool returns a new leakcheckElemTypePool.
// nolint
func newLeakcheckElemTypePool(opts leakcheckElemTypePoolOpts, backingPool elemTypePool) *leakcheckElemTypePool {
	if opts.EqualsFn == nil {
		// NB(prateek): fall-back to == in the worst case
		opts.EqualsFn = func(a, b elemType) bool {
			return a == b
		}
	}
	return &leakcheckElemTypePool{opts: opts, elemTypePool: backingPool}
}

// leakcheckElemTypePool wraps the underlying elemTypePool to make it easier to
// track leaks/allocs.
type leakcheckElemTypePool struct {
	sync.Mutex
	elemTypePool
	NumGets      int
	NumPuts      int
	PendingItems []leakcheckElemType
	AllGetItems  []leakcheckElemType

	opts leakcheckElemTypePoolOpts
}

// leakcheckElemType wraps `elemType` instances along with their last Get() paths.
type leakcheckElemType struct {
	Value         elemType
	GetStacktrace []byte // GetStacktrace is the stacktrace for the Get() of this item
}

func (p *leakcheckElemTypePool) Init() {
	p.Lock()
	defer p.Unlock()
	p.elemTypePool.Init()
}

func (p *leakcheckElemTypePool) Get() elemType {
	p.Lock()
	defer p.Unlock()

	e := p.elemTypePool.Get()
	if fn := p.opts.GetHookFn; fn != nil {
		e = fn(e)
	}

	p.NumGets++
	item := leakcheckElemType{
		Value:         e,
		GetStacktrace: debug.Stack(),
	}
	p.PendingItems = append(p.PendingItems, item)
	p.AllGetItems = append(p.AllGetItems, item)

	return e
}

func (p *leakcheckElemTypePool) Put(value elemType) {
	p.Lock()
	defer p.Unlock()

	idx := -1
	for i, item := range p.PendingItems {
		if p.opts.EqualsFn(item.Value, value) {
			idx = i
			break
		}
	}

	if idx == -1 && p.opts.DisallowUntrackedPuts {
		panic(fmt.Errorf("untracked object (%v) returned to pool", value))
	}

	if idx != -1 {
		// update slice
		p.PendingItems = append(p.PendingItems[:idx], p.PendingItems[idx+1:]...)
	}
	p.NumPuts++

	p.elemTypePool.Put(value)
}

// Check ensures there are no leaks.
func (p *leakcheckElemTypePool) Check(t *testing.T) {
	p.Lock()
	defer p.Unlock()

	require.Equal(t, p.NumGets, p.NumPuts)
	require.Empty(t, p.PendingItems)
}

type leakcheckElemTypeFn func(e leakcheckElemType)

// CheckExtended ensures there are no leaks, and executes the specified fn
func (p *leakcheckElemTypePool) CheckExtended(t *testing.T, fn leakcheckElemTypeFn) {
	p.Check(t)
	p.Lock()
	defer p.Unlock()
	for _, e := range p.AllGetItems {
		fn(e)
	}
}
