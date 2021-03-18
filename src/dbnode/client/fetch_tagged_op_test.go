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
	"context"
	"testing"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestFetchTaggedOpPool(t *testing.T) {
	p := newFetchTaggedOpPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()

	op := p.Get()
	require.Equal(t, p, op.pool)
	p.Put(op)
}

func TestFetchTaggedOp(t *testing.T) {
	p := newFetchTaggedOpPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()
	op := p.Get()

	var (
		inter interface{}
		err   error
		count int
	)

	fn := func(i interface{}, e error) {
		require.Equal(t, 0, count)
		require.Equal(t, inter, i)
		require.Equal(t, err, e)
		count++
	}
	op.update(context.Background(), rpc.FetchTaggedRequest{}, fn)
	op.CompletionFn()(inter, err)
	require.Equal(t, 1, count)
}

func TestFetchTaggedOpPoolInteraction(t *testing.T) {
	p := newFetchTaggedOpPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()
	op := p.Get()

	testPool := &testFetchTaggedOpPool{t, op, false}
	op.pool = testPool

	op.incRef()
	op.decRef()
	require.True(t, testPool.called)
	require.Nil(t, op.completionFn)
	require.Equal(t, fetchTaggedOpRequestZeroed, op.request)
}

type testFetchTaggedOpPool struct {
	t          *testing.T
	expectedOp *fetchTaggedOp
	called     bool
}

var _ fetchTaggedOpPool = &testFetchTaggedOpPool{}

func (p *testFetchTaggedOpPool) Put(o *fetchTaggedOp) {
	require.False(p.t, p.called)
	p.called = true
	require.Equal(p.t, p.expectedOp, o)
}

func (p *testFetchTaggedOpPool) Init()               { panic("not implemented") }
func (p *testFetchTaggedOpPool) Get() *fetchTaggedOp { panic("not implemented") }
